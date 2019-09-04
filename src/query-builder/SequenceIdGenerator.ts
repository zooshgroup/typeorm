import {QueryRunner} from "../query-runner/QueryRunner";

export class SequenceIdGenerator {

    constructor() {

    }

    async getId(queryRunner: QueryRunner, sequenceName: string): Promise<Number>{
        const result = await queryRunner.query("select \""+sequenceName+"\".nextval \"id\" from dummy");

        return result[0].id;
    }

}

export class SequenceParameter {

    sequenceName: string
    parameterKey : string

    constructor(parameterKey:string, sequenceName:string) {
        this.parameterKey = parameterKey;
        this.sequenceName = sequenceName;
    }

}