import { PrimaryGeneratedColumn } from '../../../../src/decorator/columns/PrimaryGeneratedColumn';
import { Entity } from '../../../../src/decorator/entity/Entity';

@Entity()
export class Post {

    @PrimaryGeneratedColumn("sequence", {sequenceName: "seq1"} )
    id: number;

}