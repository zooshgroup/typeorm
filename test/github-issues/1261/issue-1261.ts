import "reflect-metadata";
import {closeTestingConnections, createTestingConnections} from "../../utils/test-utils";
import {Connection} from "../../../src/connection/Connection";
import {BaseEntity} from "../../../src/repository/BaseEntity";
import {Bar} from "./entity/Bar";
import {PromiseUtils} from "../../../src";
import { HanaDriver } from '../../../src/driver/hana/HanaDriver';

describe("github issues > #1261 onDelete property on foreign key is not modified on sync", () => {

    let connections: Connection[];
    before(async () => connections = await createTestingConnections({
        entities: [__dirname + "/entity/*{.js,.ts}"],
    }));
    after(() => closeTestingConnections(connections));

    it("should modify onDelete property on foreign key on sync", () => PromiseUtils.runInSequence(connections, async connection => {

        if (connection.driver instanceof HanaDriver) { // TODO HANA - changeColumn() missing
            return;
        }

        await connection.synchronize();
        BaseEntity.useConnection(connection);

        const queryRunner = connection.createQueryRunner();
        let table = await queryRunner.getTable("bar");
        table!.foreignKeys[0].onDelete!.should.be.equal("SET NULL");

        const metadata = connection.getMetadata(Bar);
        metadata.foreignKeys[0].onDelete = "CASCADE";
        await connection.synchronize();

        table = await queryRunner.getTable("bar");
        table!.foreignKeys[0].onDelete!.should.be.equal("CASCADE");

        await queryRunner.release();

    }));

});
