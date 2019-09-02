/**
 * Thrown when an operation is not supported by the driver.
 */
export class OperationNotSupportedError extends Error {
    name = "OperationNotSupportedError";

    constructor(message = `The operation is not supported by the driver.`) {
        super();
        Object.setPrototypeOf(this, OperationNotSupportedError.prototype);
        this.message = message;        
    }
}
