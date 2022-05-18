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

export declare function initializeDatabase (config: DatabaseInitializationConfiguration): Promise<void>;
