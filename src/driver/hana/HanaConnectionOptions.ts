import { BaseConnectionOptions } from "../../connection/BaseConnectionOptions";
import { HanaConnectionCredentialsOptions } from "./HanaConnectionCredentialsOptions";

/**
 * Hana specific connection options.
 */
export interface HanaConnectionOptions extends BaseConnectionOptions, HanaConnectionCredentialsOptions {

    /**
     * Database type.
     */
    readonly type: "hana-column";

    /**
     * Schema name. By default is "public".
     */
    readonly schema?: string;

    /**
    * Connection url where perform connection to.
    */
    readonly url?: string;

    /**
     * Database host.
     */
    readonly host?: string;

    /**
     * Database host port.
     */
    readonly port?: number;

    /**
     * Database name to connect to.
     */
    readonly database?: string;

    /**
     * An optional object/dictionary with the any of the properties
     */
    readonly pool?: {

        /**
         * Maximum number of resources to create at any given time. (default=10)
         */
        readonly max?: number;

        /**
         * Minimum number of resources to keep in pool at any given time. If this is set >= max, the pool will silently
         * set the min to equal max. (default=2)
         */
        readonly min?: number;

        /**
         * Should the pool start creating resources etc once the constructor is called, (default false)
         */
        readonly autostart?: number;

    };

}