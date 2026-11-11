//! TLS options extracted from a Java `TlsOptions` object.
//!
//! The extracted values are converted to a `TlsSettings` on the JNI thread. File
//! paths stay paths, so the core reads them again whenever it opens a channel.

use databricks_zerobus_ingest_sdk::TlsSettings;
use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use std::path::PathBuf;

/// TLS settings read from a Java `TlsOptions` object.
pub struct ExtractedTlsOptions {
    trust_system_roots: bool,
    ca_certificate_pems: Vec<Vec<u8>>,
    ca_certificate_files: Vec<PathBuf>,
    client_identity: Option<ClientIdentity>,
    domain_name: Option<String>,
}

enum ClientIdentity {
    Pem { cert: Vec<u8>, key: Vec<u8> },
    Files { cert: PathBuf, key: PathBuf },
}

impl ExtractedTlsOptions {
    pub fn into_settings(self) -> TlsSettings {
        let mut settings = TlsSettings::new();
        if !self.trust_system_roots {
            settings = settings.without_native_roots();
        }
        for pem in self.ca_certificate_pems {
            settings = settings.with_ca_certificate_pem(pem);
        }
        for path in self.ca_certificate_files {
            settings = settings.with_ca_certificate_file(path);
        }
        settings = match self.client_identity {
            Some(ClientIdentity::Pem { cert, key }) => settings.with_client_identity_pem(cert, key),
            Some(ClientIdentity::Files { cert, key }) => {
                settings.with_client_identity_files(cert, key)
            }
            None => settings,
        };
        if let Some(domain_name) = self.domain_name {
            settings = settings.with_domain_name(domain_name);
        }
        settings
    }
}

/// Reads a Java `TlsOptions` object. The Java builder guarantees that each identity
/// is either PEM data or a pair of files.
pub fn extract_tls_options(
    env: &mut JNIEnv,
    options: &JObject,
) -> Result<ExtractedTlsOptions, jni::errors::Error> {
    let trust_system_roots = env
        .call_method(options, "isTrustSystemRoots", "()Z", &[])?
        .z()?;
    let ca_certificate_pems = read_string_list(env, options, "getCaCertificatePems")?
        .into_iter()
        .map(String::into_bytes)
        .collect();
    let ca_certificate_files = read_string_list(env, options, "getCaCertificateFiles")?
        .into_iter()
        .map(PathBuf::from)
        .collect();

    let client_certificate_pem = read_nullable_string(env, options, "getClientCertificatePem")?;
    let client_key_pem = read_nullable_string(env, options, "getClientKeyPem")?;
    let client_certificate_file = read_nullable_string(env, options, "getClientCertificateFile")?;
    let client_key_file = read_nullable_string(env, options, "getClientKeyFile")?;
    let client_identity = match (
        client_certificate_pem,
        client_key_pem,
        client_certificate_file,
        client_key_file,
    ) {
        (Some(cert), Some(key), _, _) => Some(ClientIdentity::Pem {
            cert: cert.into_bytes(),
            key: key.into_bytes(),
        }),
        (_, _, Some(cert), Some(key)) => Some(ClientIdentity::Files {
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        }),
        _ => None,
    };

    let domain_name = read_nullable_string(env, options, "getDomainName")?;

    Ok(ExtractedTlsOptions {
        trust_system_roots,
        ca_certificate_pems,
        ca_certificate_files,
        client_identity,
        domain_name,
    })
}

fn read_string_list(
    env: &mut JNIEnv,
    options: &JObject,
    getter: &str,
) -> Result<Vec<String>, jni::errors::Error> {
    let list = env
        .call_method(options, getter, "()Ljava/util/List;", &[])?
        .l()?;
    let size = env.call_method(&list, "size", "()I", &[])?.i()?;
    let mut strings = Vec::with_capacity(size.max(0) as usize);
    for index in 0..size {
        let item = env
            .call_method(&list, "get", "(I)Ljava/lang/Object;", &[JValue::Int(index)])?
            .l()?;
        let item = JString::from(item);
        let value: String = env.get_string(&item)?.into();
        env.delete_local_ref(item)?;
        strings.push(value);
    }
    Ok(strings)
}

fn read_nullable_string(
    env: &mut JNIEnv,
    options: &JObject,
    getter: &str,
) -> Result<Option<String>, jni::errors::Error> {
    let value = env
        .call_method(options, getter, "()Ljava/lang/String;", &[])?
        .l()?;
    if value.is_null() {
        return Ok(None);
    }
    let value = JString::from(value);
    let string: String = env.get_string(&value)?.into();
    env.delete_local_ref(value)?;
    Ok(Some(string))
}

#[cfg(test)]
mod tests {
    use super::{ClientIdentity, ExtractedTlsOptions};
    use async_trait::async_trait;
    use databricks_zerobus_ingest_sdk::{HeadersProvider, ZerobusResult, ZerobusSdk};
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use zerobus_test_support::mock_grpc::{start_mock_mtls_server, MockResponse};

    const TABLE_NAME: &str = "test_catalog.test_schema.test_table";
    /// Base64 of `b"zerobus\0bin"`, the value the mock server decodes from `x-test-bin`.
    const BINARY_HEADER_BASE64: &str = "emVyb2J1cwBiaW4=";
    const BINARY_HEADER_BYTES: &[u8] = b"zerobus\0bin";

    struct BinaryHeaders;

    #[async_trait]
    impl HeadersProvider for BinaryHeaders {
        async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::from([
                ("authorization", "Bearer test_token".to_string()),
                ("x-databricks-zerobus-table-name", "test_table".to_string()),
                ("x-test-bin", BINARY_HEADER_BASE64.to_string()),
            ]))
        }
    }

    /// Mutual TLS with the CA and the client identity read from files, plus a binary
    /// header, through the SDK built from the extracted options.
    #[tokio::test]
    async fn file_based_tls_options_open_a_mutual_tls_stream(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let CertifiedKey {
            cert: client_cert,
            key_pair: client_key,
        } = generate_simple_self_signed(vec!["zerobus-test-client".to_string()])?;
        let client_cert_pem = client_cert.pem();
        let (mock_server, server_url, server_ca_pem) =
            start_mock_mtls_server(client_cert_pem.as_bytes()).await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "tls_options_stream".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let dir = tempfile::tempdir()?;
        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("tls.crt");
        let key_path = dir.path().join("tls.key");
        std::fs::write(&ca_path, &server_ca_pem)?;
        std::fs::write(&cert_path, client_cert_pem.as_bytes())?;
        std::fs::write(&key_path, client_key.serialize_pem())?;

        let extracted = ExtractedTlsOptions {
            trust_system_roots: false,
            ca_certificate_pems: Vec::new(),
            ca_certificate_files: vec![ca_path],
            client_identity: Some(ClientIdentity::Files {
                cert: cert_path,
                key: key_path,
            }),
            domain_name: Some("localhost".to_string()),
        };
        let sdk = ZerobusSdk::builder()
            .endpoint(server_url)
            .unity_catalog_url("https://mock-uc.com")
            .tls_config(Arc::new(extracted.into_settings()))
            .build()?;

        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(BinaryHeaders))
            .json()
            .max_inflight_requests(10)
            .recovery(false)
            .build()
            .await?;
        stream
            .ingest_record_offset(r#"{"id": 1, "message": "tls options test"}"#.to_string())
            .await?;
        stream.flush().await?;
        stream.close().await?;

        assert_eq!(
            mock_server.get_test_bin_headers().await,
            vec![Some(BINARY_HEADER_BYTES.to_vec())]
        );
        Ok(())
    }

    fn options(trust_system_roots: bool) -> ExtractedTlsOptions {
        ExtractedTlsOptions {
            trust_system_roots,
            ca_certificate_pems: Vec::new(),
            ca_certificate_files: Vec::new(),
            client_identity: None,
            domain_name: None,
        }
    }

    #[test]
    fn maps_every_field_onto_the_core_settings() {
        let mut extracted = options(false);
        extracted.ca_certificate_pems = vec![b"pem-ca".to_vec()];
        extracted.ca_certificate_files = vec![PathBuf::from("/etc/zerobus/ca.pem")];
        extracted.client_identity = Some(ClientIdentity::Files {
            cert: PathBuf::from("/etc/zerobus/tls.crt"),
            key: PathBuf::from("/etc/zerobus/tls.key"),
        });
        extracted.domain_name = Some("zerobus.example.com".to_string());

        let debug = format!("{:?}", extracted.into_settings());

        assert!(debug.contains("native_roots: false"), "{debug}");
        assert!(debug.contains("ca_certificates: 2"), "{debug}");
        assert!(
            debug.contains("client_identity: Some(\"<redacted>\")"),
            "{debug}"
        );
        assert!(debug.contains("zerobus.example.com"), "{debug}");
    }

    #[test]
    fn default_options_keep_system_roots_and_no_identity() {
        let debug = format!("{:?}", options(true).into_settings());

        assert!(debug.contains("native_roots: true"), "{debug}");
        assert!(debug.contains("ca_certificates: 0"), "{debug}");
        assert!(debug.contains("client_identity: None"), "{debug}");
        assert!(debug.contains("domain_name: None"), "{debug}");
    }

    #[test]
    fn pem_identity_is_mapped_as_pem() {
        let mut extracted = options(true);
        extracted.client_identity = Some(ClientIdentity::Pem {
            cert: b"cert".to_vec(),
            key: b"key".to_vec(),
        });

        let debug = format!("{:?}", extracted.into_settings());

        assert!(
            debug.contains("client_identity: Some(\"<redacted>\")"),
            "{debug}"
        );
        assert!(
            !debug.contains("\"cert\"") && !debug.contains("\"key\""),
            "{debug}"
        );
    }
}
