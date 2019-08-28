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

}