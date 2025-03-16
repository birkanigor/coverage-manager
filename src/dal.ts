import { Pool, QueryResult, QueryResultRow, PoolClient } from 'pg';
import { EnvReader } from './env';
import { Logger as Log4jsLogger } from "log4js";

type QueryParams = any[];

export class PostgresQueryRunner {
    private pool: Pool;
    private log: Log4jsLogger ;

    constructor(log: Log4jsLogger) {
        const envReader = new EnvReader();
        this.log = log;

        this.pool = new Pool({
            user: envReader.getValue("DB_USER"),
            password: envReader.getValue("DB_PASSWORD"),
            host: envReader.getValue("DB_HOST"),
            port: parseInt(envReader.getValue("DB_PORT") || '5432', 10),
            database: envReader.getValue("DB_NAME"),
        });

        this.pool.on('connect', async (client: PoolClient) => {
            try {
                await client.query('SET search_path TO cm_conf, public');
            } catch (error) {
                console.error('Error setting search_path:', error);
            }
        });
    }

    /**
     * Executes a SQL query with optional parameters.
     * @param query - The SQL query string.
     * @param params - The optional array of query parameters.
     * @param loggable - The optional boolean flag to write the query to the log ( default ) or not
     * @returns A promise that resolves to the query result.
     */
    public async executeQuery<T extends QueryResultRow>(query: string, params: QueryParams = [], loggable: boolean = true): Promise<T[]> {
        try {
            if(loggable){
                this.log.debug(query, params)
            }

            const result:QueryResult = await this.pool.query<T>(query, params);
            return result.rows;
        } catch (error) {
            this.log.error('Error executing query:', error);
            throw error;
        }
    }

    /**
     * Closes the database connection pool.
     */
    public async closeConnection(): Promise<void> {
        try {
            await this.pool.end();
            this.log.log('Database connection pool closed.');
        } catch (error) {
            this.log.error('Error closing connection pool:', error);
        }
    }
}
