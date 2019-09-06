
import {Person} from "./Person";
import {ChildEntity} from '../../../../../../src/decorator/entity/ChildEntity';
import {Column} from '../../../../../../src/decorator/columns/Column';

@ChildEntity()
export class Employee extends Person {

    @Column()
    salary: number;

}
