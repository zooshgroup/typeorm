import "reflect-metadata";
import {Connection} from "../../../src/connection/Connection";
import {closeTestingConnections, createTestingConnections, reloadTestingDatabases} from "../../utils/test-utils";
import {PromiseUtils} from "../../../src";
import {Teacher} from "./entity/Teacher";
import {UniqueMetadata} from "../../../src/metadata/UniqueMetadata";
import {MysqlDriver} from "../../../src/driver/mysql/MysqlDriver";
import {Post} from "./entity/Post";
import {AbstractSqliteDriver} from "../../../src/driver/sqlite-abstract/AbstractSqliteDriver";
import {IndexMetadata} from "../../../src/metadata/IndexMetadata";
import { HanaColumnDriver } from '../../../src/driver/hana/HanaColumnDriver';

describe("schema builder > change unique constraint", () => {

    let connections: Connection[];
    before(async () => {
        connections = await createTestingConnections({
            entities: [__dirname + "/entity/*{.js,.ts}"],
            schemaCreate: true,
            dropSchema: true,
        });
    });
    beforeEach(() => reloadTestingDatabases(connections));
    after(() => closeTestingConnections(connections));

    it("should correctly add new unique constraint", () => PromiseUtils.runInSequence(connections, async connection => {
        if (connection.driver instanceof HanaColumnDriver) {
            return;
        }

        const teacherMetadata = connection.getMetadata(Teacher);
        const nameColumn = teacherMetadata.findColumnWithPropertyName("name")!;
        let uniqueIndexMetadata: IndexMetadata|undefined = undefined;
        let uniqueMetadata: UniqueMetadata|undefined = undefined;

        // Mysql stores unique constraints as unique indices.
        if (connection.driver instanceof MysqlDriver) {
            uniqueIndexMetadata = new IndexMetadata({
                entityMetadata: teacherMetadata,
                columns: [nameColumn],
                args: {
                    target: Teacher,
                    unique: true,
                    synchronize: true
                }
            });
            uniqueIndexMetadata.build(connection.namingStrategy);
            teacherMetadata.indices.push(uniqueIndexMetadata);

        } else {
            uniqueMetadata = new UniqueMetadata({
                entityMetadata: teacherMetadata,
                columns: [nameColumn],
                args: {
                    target: Teacher
                }
            });
            uniqueMetadata.build(connection.namingStrategy);
            teacherMetadata.uniques.push(uniqueMetadata);
        }

        await connection.synchronize();

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("teacher");
        await queryRunner.release();

        if (connection.driver instanceof MysqlDriver) {
            table!.indices.length.should.be.equal(1);
            table!.indices[0].isUnique!.should.be.true;

            // revert changes
            teacherMetadata.indices.splice(teacherMetadata.indices.indexOf(uniqueIndexMetadata!), 1);

        } else {
            table!.uniques.length.should.be.equal(1);

            // revert changes
            teacherMetadata.uniques.splice(teacherMetadata.uniques.indexOf(uniqueMetadata!), 1);
        }
    }));

    it("should correctly change unique constraint", () => PromiseUtils.runInSequence(connections, async connection => {
        if (connection.driver instanceof HanaColumnDriver) {
            return;
        }

        // Sqlite does not store unique constraint name
        if (connection.driver instanceof AbstractSqliteDriver)
            return;

        const postMetadata = connection.getMetadata(Post);

        // Mysql stores unique constraints as unique indices.
        if (connection.driver instanceof MysqlDriver) {
            const uniqueIndexMetadata = postMetadata.indices.find(i => i.columns.length === 2 && i.isUnique === true);
            uniqueIndexMetadata!.name = "changed_unique";

        } else {
            const uniqueMetadata = postMetadata.uniques.find(uq => uq.columns.length === 2);
            uniqueMetadata!.name = "changed_unique";
        }

        await connection.synchronize();

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("post");
        await queryRunner.release();

        if (connection.driver instanceof MysqlDriver) {
            const tableIndex = table!.indices.find(index => index.columnNames.length === 2 && index.isUnique === true);
            tableIndex!.name!.should.be.equal("changed_unique");

            // revert changes
            const uniqueIndexMetadata = postMetadata.indices.find(i => i.name === "changed_unique");
            uniqueIndexMetadata!.name = connection.namingStrategy.indexName(table!, uniqueIndexMetadata!.columns.map(c => c.databaseName));

        } else {
            const tableUnique = table!.uniques.find(unique => unique.columnNames.length === 2);
            tableUnique!.name!.should.be.equal("changed_unique");

            // revert changes
            const uniqueMetadata = postMetadata.uniques.find(i => i.name === "changed_unique");
            uniqueMetadata!.name = connection.namingStrategy.uniqueConstraintName(table!, uniqueMetadata!.columns.map(c => c.databaseName));
        }

    }));

    it("should correctly drop removed unique constraint", () => PromiseUtils.runInSequence(connections, async connection => {
        if (connection.driver instanceof HanaColumnDriver) {
            return;
        }

        const postMetadata = connection.getMetadata(Post);

        // Mysql stores unique constraints as unique indices.
        if (connection.driver instanceof MysqlDriver) {
            const index = postMetadata!.indices.find(i => i.columns.length === 2 && i.isUnique === true);
            postMetadata!.indices.splice(postMetadata!.indices.indexOf(index!), 1);

        } else  {
            const unique = postMetadata!.uniques.find(u => u.columns.length === 2);
            postMetadata!.uniques.splice(postMetadata!.uniques.indexOf(unique!), 1);
        }

        await connection.synchronize();

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("post");
        await queryRunner.release();

        if (connection.driver instanceof MysqlDriver) {
            table!.indices.length.should.be.equal(1);

        } else {
            table!.uniques.length.should.be.equal(1);
        }
    }));

});
