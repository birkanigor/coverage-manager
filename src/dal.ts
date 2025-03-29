import { Pool, QueryResult, QueryResultRow, PoolClient } from 'pg';
import { EnvReader } from './env';
import { Logger as Log4jsLogger } from "log4js";

type QueryParams = any[];

export class PostgresQueryRunner {
    private pool: Pool;
    private log: Log4jsLogger ;

    private typeMapping: { [key: number]: string } = {
        16: 'boolean',
        20: 'bigint',
        23: 'integer',
        25: 'text',
        700: 'real',
        701: 'double precision',
        1043: 'varchar',
        1082: 'date',
        1114: 'timestamp without time zone',
        1184: 'timestamp with time zone',
        // Add more OIDs if needed
    };

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

        this.log.debug(`path : ${envReader.getEnvPath()}`)
        this.log.debug(JSON.stringify(this.pool.options));

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
    public async executeQuery<T extends QueryResultRow>(query: string, params: QueryParams = [], loggable: boolean = true): Promise<{ rows: T[], columns: { name: string; dataType: string }[] }> {
        try {
            if(loggable) {
                this.log.debug(`query: ${query} , params : ${JSON.stringify(params)}`);
            }

            const result: QueryResult = await this.pool.query<T>(query, params);

            const columns = result.fields.map(field => ({
                name: field.name,
                dataType: this.typeMapping[field.dataTypeID] || `Unknown(${field.dataTypeID})`
            }));

            return { rows: result.rows, columns };
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
