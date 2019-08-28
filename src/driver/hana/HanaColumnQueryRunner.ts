import { QueryRunner } from "../../query-runner/QueryRunner";
import { TransactionAlreadyStartedError } from "../../error/TransactionAlreadyStartedError";
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { Table } from "../../schema-builder/table/Table";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
import { TableIndex } from "../../schema-builder/table/TableIndex";
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError";
import { View } from "../../schema-builder/view/View";
import { Query } from "../Query";
import { HanaColumnDriver } from "./HanaColumnDriver";
import { ReadStream } from "../../platform/PlatformTools";
import { QueryFailedError } from "../../error/QueryFailedError";
import { TableUnique } from "../../schema-builder/table/TableUnique";
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner";
import { TableCheck } from "../../schema-builder/table/TableCheck";
import { IsolationLevel } from "../types/IsolationLevel";
import { TableExclusion } from "../../schema-builder/table/TableExclusion";
import { Broadcaster } from "../../subscriber/Broadcaster";
import { OperationNotSupportedError } from '../../error/OperationNotSupportedError';

/**
 * Runs queries on a single mysql database connection.
 */
export class HanaColumnQueryRunner extends BaseQueryRunner implements QueryRunner {

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Database driver used by connection.
     */
    driver: HanaColumnDriver;

    // -------------------------------------------------------------------------
    // Protected Properties
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: HanaColumnDriver) {
        super();
        this.driver = driver;
        this.connection = driver.connection;
        this.broadcaster = new Broadcaster(this);
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    connect(): Promise<any> {
        return Promise.resolve(this.driver.databaseConnection);
    }

    /**
     * Releases used database connection.
     * We just clear loaded tables and sql in memory, because sqlite do not support multiple connections thus query runners.
     */
    release(): Promise<void> {
        this.loadedTables = [];
        this.clearSqlMemory();
        return Promise.resolve();
    }

    /**
     * Starts transaction on the current connection.
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (this.isTransactionActive)
            throw new TransactionAlreadyStartedError();

        this.isTransactionActive = true;
        if (isolationLevel) {
            // TODO UNCOMMITTED 
            await this.query("SET TRANSACTION ISOLATION LEVEL " + isolationLevel);
        }
    }

    /**
     * Commits transaction.
     * Error will be thrown if transaction was not started.
     */
    async commitTransaction(): Promise<void> {
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        this.driver.databaseConnection.commit();
        this.isTransactionActive = false;
    }

    /**
     * Rollbacks transaction.
     * Error will be thrown if transaction was not started.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        this.driver.databaseConnection.rollback();
        this.isTransactionActive = false;
    }

    /**
     * Executes a raw SQL query.
     */
    query(query: string, parameters?: any[]): Promise<any> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError();

        return new Promise(async (ok, fail) => {
            try {
                const databaseConnection = await this.connect();
                this.driver.connection.logger.logQuery(query, parameters, this);
                const queryStartTime = +new Date();
                databaseConnection.exec(query, parameters, (err: any, result: any) => {

                    // log slow queries if maxQueryExecution time is set
                    const maxQueryExecutionTime = this.driver.connection.options.maxQueryExecutionTime;
                    const queryEndTime = +new Date();
                    const queryExecutionTime = queryEndTime - queryStartTime;
                    if (maxQueryExecutionTime && queryExecutionTime > maxQueryExecutionTime)
                        this.driver.connection.logger.logQuerySlow(queryExecutionTime, query, parameters, this);

                    if (err) {
                        this.driver.connection.logger.logQueryError(err, query, parameters, this);
                        return fail(new QueryFailedError(query, parameters, err));
                    }

                    ok(result);
                });

            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * Returns raw data stream.
     */
    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<ReadStream> {
        throw new OperationNotSupportedError();
    }

    /**
     * Returns all available database names including system databases.
     */
    async getDatabases(): Promise<string[]> {
        return Promise.resolve([]);
    }

    /**
     * Returns all available schema names including system schemas.
     * If database parameter specified, returns schemas of that database.
     */
    async getSchemas(database?: string): Promise<string[]> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if database with the given name exist.
     */
    async hasDatabase(database: string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if schema with the given name exist.
     */
    async hasSchema(schema: string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if table with the given name exist in the database.
     */
    async hasTable(tableOrName: Table | string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Checks if column with the given name exist in the given table.
     */
    async hasColumn(tableOrName: Table | string, column: TableColumn | string): Promise<boolean> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new database.
     */
    async createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops database.
     */
    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new table schema.
     */
    async createSchema(schema: string, ifNotExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops table schema.
     */
    async dropSchema(schemaPath: string, ifExist?: boolean): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new table.
     */
    async createTable(table: Table, ifNotExist: boolean = false, createForeignKeys: boolean = true): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drop the table.
     */
    async dropTable(target: Table | string, ifExist?: boolean, dropForeignKeys: boolean = true): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new view.
     */
    async createView(view: View): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops the view.
     */
    async dropView(target: View | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Renames a table.
     */
    async renameTable(oldTableOrName: Table | string, newTableName: string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new column from the column in the table.
     */
    async addColumn(tableOrName: Table | string, column: TableColumn): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new columns from the column in the table.
     */
    async addColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Renames column in the given table.
     */
    async renameColumn(tableOrName: Table | string, oldTableColumnOrName: TableColumn | string, newTableColumnOrName: TableColumn | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Changes a column in the table.
     */
    async changeColumn(tableOrName: Table | string, oldColumnOrName: TableColumn | string, newColumn: TableColumn): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Changes a column in the table.
     */
    async changeColumns(tableOrName: Table | string, changedColumns: { newColumn: TableColumn, oldColumn: TableColumn }[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops column in the table.
     */
    async dropColumn(tableOrName: Table | string, columnOrName: TableColumn | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops the columns in the table.
     */
    async dropColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new primary key.
     */
    async createPrimaryKey(tableOrName: Table | string, columnNames: string[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Updates composite primary keys.
     */
    async updatePrimaryKeys(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops a primary key.
     */
    async dropPrimaryKey(tableOrName: Table | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new unique constraint.
     */
    async createUniqueConstraint(tableOrName: Table | string, uniqueConstraint: TableUnique): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new unique constraints.
     */
    async createUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an unique constraint.
     */
    async dropUniqueConstraint(tableOrName: Table | string, uniqueOrName: TableUnique | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an unique constraints.
     */
    async dropUniqueConstraints(tableOrName: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new check constraint.
     */
    async createCheckConstraint(tableOrName: Table | string, checkConstraint: TableCheck): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new check constraints.
     */
    async createCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops check constraint.
     */
    async dropCheckConstraint(tableOrName: Table | string, checkOrName: TableCheck | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops check constraints.
     */
    async dropCheckConstraints(tableOrName: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new exclusion constraint.
     */
    async createExclusionConstraint(tableOrName: Table | string, exclusionConstraint: TableExclusion): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new exclusion constraints.
     */
    async createExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops exclusion constraint.
     */
    async dropExclusionConstraint(tableOrName: Table | string, exclusionOrName: TableExclusion | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops exclusion constraints.
     */
    async dropExclusionConstraints(tableOrName: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new foreign key.
     */
    async createForeignKey(tableOrName: Table | string, foreignKey: TableForeignKey): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new foreign keys.
     */
    async createForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops a foreign key.
     */
    async dropForeignKey(tableOrName: Table | string, foreignKeyOrName: TableForeignKey | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops a foreign keys from the table.
     */
    async dropForeignKeys(tableOrName: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new index.
     */
    async createIndex(tableOrName: Table | string, index: TableIndex): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Creates a new indices
     */
    async createIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an index.
     */
    async dropIndex(tableOrName: Table | string, indexOrName: TableIndex | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Drops an indices from the table.
     */
    async dropIndices(tableOrName: Table | string, indices: TableIndex[]): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Clears all table contents.
     * Note: this operation uses SQL's TRUNCATE query which cannot be reverted in transactions.
     */
    async clearTable(tableOrName: Table | string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    /**
     * Removes all tables from the currently connected database.
     * Be careful using this method and avoid using it in production or migrations
     * (because it can clear all your database).
     */
    async clearDatabase(database?: string): Promise<void> {
        throw new OperationNotSupportedError();
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Returns current database.
     */
    protected async getCurrentDatabase(): Promise<string> {
        throw new OperationNotSupportedError();
    }

    protected async loadViews(viewNames: string[]): Promise<View[]> {
        return [];
    }

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     */
    protected async loadTables(tableNames: string[]): Promise<Table[]> {
        const tableColumnID = new TableColumn();
        tableColumnID.name = "ID";
        tableColumnID.type = "integer";

        const tableColumnString = new TableColumn();
        tableColumnString.name = "TEST";
        tableColumnString.type = "varchar";

        const table = new Table();
        table.name = "TEST";
        table.columns = [tableColumnID, tableColumnString];

        return Promise.all([table]);
    }

    /**
     * Builds create table sql
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop table sql
     */
    protected dropTableSql(tableOrName: Table | string): Query {
        throw new OperationNotSupportedError();
    }

    protected createViewSql(view: View): Query {
        throw new OperationNotSupportedError();
    }

    protected async insertViewDefinitionSql(view: View): Promise<Query> {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop view sql.
     */
    protected dropViewSql(viewOrPath: View | string): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds remove view sql.
     */
    protected async deleteViewDefinitionSql(viewOrPath: View | string): Promise<Query> {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds create index sql.
     */
    protected createIndexSql(table: Table, index: TableIndex): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop index sql.
     */
    protected dropIndexSql(table: Table, indexOrName: TableIndex | string): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds create primary key sql.
     */
    protected createPrimaryKeySql(table: Table, columnNames: string[]): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop primary key sql.
     */
    protected dropPrimaryKeySql(table: Table): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds create foreign key sql.
     */
    protected createForeignKeySql(table: Table, foreignKey: TableForeignKey): Query {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds drop foreign key sql.
     */
    protected dropForeignKeySql(table: Table, foreignKeyOrName: TableForeignKey | string): Query {
        throw new OperationNotSupportedError();
    }

    protected parseTableName(target: Table | string) {
        throw new OperationNotSupportedError();
    }

    /**
     * Escapes given table or view path.
     */
    protected escapePath(target: Table | View | string, disableEscape?: boolean): string {
        throw new OperationNotSupportedError();
    }

    /**
     * Builds a part of query to create/change a column.
     */
    protected buildCreateColumnSql(column: TableColumn, skipPrimary: boolean, skipName: boolean = false) {
        throw new OperationNotSupportedError();
    }

}
