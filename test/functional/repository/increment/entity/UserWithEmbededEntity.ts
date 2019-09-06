import { Column } from '../../../../../src/decorator/columns/Column';
import { Entity } from '../../../../../src/decorator/entity/Entity';
import { PrimaryColumn } from '../../../../../src/decorator/columns/PrimaryColumn';

class FriendStats {
    @Column({ default: 0 })
    count: number;

    @Column({ default: 0 })
    sent: number;

    @Column({ default: 0 })
    received: number;
}

@Entity()
export class UserWithEmbededEntity {

    @PrimaryColumn()
    id: number;

    @Column(type => FriendStats)
    friend: FriendStats;
}
