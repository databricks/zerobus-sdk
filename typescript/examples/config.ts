export function requiredEnv(name: string): string {
    const value = process.env[name];
    if (value) {
        return value;
    }
    throw new Error(`${name} must be set; see typescript/examples/README.md`);
}
