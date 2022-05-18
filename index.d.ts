import { PoolConfig } from "mariadb";

declare type DatabaseInitializationConfiguration = {
    auth: PoolConfig;
    definitionFiles?: string[];
    initialDataFiles?: string[];
    meta?: {
        dataPath?: string;
        definitionPath?: string;
        requiredMariaMajorVersion?: number;
    }
}

export default function initializeDatabase (config: DatabaseInitializationConfiguration): Promise<void>;
