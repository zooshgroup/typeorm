import "reflect-metadata";
import {expect} from "chai";

import {Post} from "./entity/Post";
import { Connection } from "../../../src/connection/Connection";
import { createTestingConnections, reloadTestingDatabases, closeTestingConnections } from '../../utils/test-utils';

describe("sequence", () => {

    let connections: Connection[];
    before(async () => connections = await createTestingConnections({
        entities: [__dirname + "/entity/*{.js,.ts}"]
    }));
    beforeEach(() => reloadTestingDatabases(connections));
    after(() => closeTestingConnections(connections));

    it("should persist with generated sequence", () => Promise.all(connections.map(async connection => {

        const postRepository = connection.getRepository(Post);
        const queryRunner = connection.createQueryRunner();
        const postTable = await queryRunner.getTable("post");
        await queryRunner.release();

        const post = new Post();
        await postRepository.save(post);
        const loadedPost = await postRepository.findOne(1);
        expect(loadedPost!.id).to.be.exist;
        postTable!.findColumnByName("id")!.type.should.be.equal("bigint");
        expect(loadedPost!.id).to.equal(1);
        expect(post!.id).to.equal(1);

        const post2 = new Post();
        post2.id = 20;
        await postRepository.save(post2);
        const loadedPost2 = await postRepository.findOne(20);
        expect(loadedPost2!.id).to.equal(20);

    })));
});
