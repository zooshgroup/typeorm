import "reflect-metadata";
import {CockroachDriver} from "../../../../src/driver/cockroachdb/CockroachDriver";
import {closeTestingConnections, createTestingConnections, reloadTestingDatabases} from "../../../utils/test-utils";
import {Connection} from "../../../../src/connection/Connection";
import {expect} from "chai";
import {EntityMetadata} from "../../../../src/metadata/EntityMetadata";
import {IndexMetadata} from "../../../../src/metadata/IndexMetadata";

import {Person} from "./entity/Person";
import { HanaColumnDriver } from '../../../../src/driver/hana/HanaColumnDriver';

describe("database schema > indices > reading index from entity and updating database", () => {

    let connections: Connection[];
    before(async () => connections = await createTestingConnections({
        entities: [__dirname + "/entity/*{.js,.ts}"],
    }));
    beforeEach(() => reloadTestingDatabases(connections));
    after(() => closeTestingConnections(connections));

    it("should create a non unique index with 2 columns", () => Promise.all(connections.map(async connection => {

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("person");
        await queryRunner.release();

        expect(table!.indices.length).to.be.equal(1);
        expect(table!.indices[0].name).to.be.equal("IDX_TEST");
        expect(table!.indices[0].isUnique).to.be.false;
        expect(table!.indices[0].columnNames.length).to.be.equal(2);
        expect(table!.indices[0].columnNames).to.deep.include.members(["firstname", "lastname"]);

    })));

    it("should update the index to be unique", () => Promise.all(connections.map(async connection => {

        if (connection.driver instanceof HanaColumnDriver) { // TODO HANA - changeColumn() missing
            //return;
        }

        const entityMetadata = connection.entityMetadatas.find(x => x.name === "Person");
        const indexMetadata = entityMetadata!.indices.find(x => x.name === "IDX_TEST");
        indexMetadata!.isUnique = true;

        await connection.synchronize(false);

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("person");
        await queryRunner.release();

        // CockroachDB stores unique indices as UNIQUE constraints
        if (connection.driver instanceof CockroachDriver) {
            expect(table!.uniques.length).to.be.equal(1);
            expect(table!.uniques[0].name).to.be.equal("IDX_TEST");
            expect(table!.uniques[0].columnNames.length).to.be.equal(2);
            expect(table!.uniques[0].columnNames).to.deep.include.members(["firstname", "firstname"]);
        } else {
            expect(table!.indices.length).to.be.equal(1);
            expect(table!.indices[0].name).to.be.equal("IDX_TEST");
            expect(table!.indices[0].isUnique).to.be.true;
            expect(table!.indices[0].columnNames.length).to.be.equal(2);
            expect(table!.indices[0].columnNames).to.deep.include.members(["firstname", "firstname"]);
        }

    })));

    it("should update the index swaping the 2 columns", () => Promise.all(connections.map(async connection => {

        if (connection.driver instanceof HanaColumnDriver) { // TODO HANA - changeColumn() missing
            return;
        }

        const entityMetadata = connection.entityMetadatas.find(x => x.name === "Person");
        entityMetadata!.indices = [new IndexMetadata({
            entityMetadata: <EntityMetadata>entityMetadata,
            args: {
                target: Person,
                name: "IDX_TEST",
                columns: ["lastname", "firstname"],
                unique: false,
                synchronize: true
            }
        })];
        entityMetadata!.indices.forEach(index => index.build(connection.namingStrategy));

        await connection.synchronize(false);

        const queryRunner = connection.createQueryRunner();
        const table = await queryRunner.getTable("person");
        await queryRunner.release();

        expect(table!.indices.length).to.be.equal(1);
        expect(table!.indices[0].name).to.be.equal("IDX_TEST");
        expect(table!.indices[0].isUnique).to.be.false;
        expect(table!.indices[0].columnNames.length).to.be.equal(2);
        expect(table!.indices[0].columnNames).to.deep.include.members(["firstname", "lastname"]);

    })));
});
