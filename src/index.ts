import { Subspace, directory as fdbDirectory, Database, Transaction } from 'foundationdb';
// TODO(tabeth): can be combined with above.
import * as fdb from "foundationdb"


// TODO(tabeth): Change into a flag
const FDB_API_VERSION = 710; // Choose your FDB API version

// ConcreteDB is a DynamoDB API compatible database, backed by FoundationDB.
// It hopes to have the same functionality, including DynamoDB streams.
export class ConcreteDB {

    private db: Database;
    private rootDirectory: Subspace | null = null;
    private tableMetadataDir: Subspace | null = null;
    private tableDataDir: Subspace | null = null;



    constructor(clusterFilePath?: string) {
        this.db = ConcreteDB.getDatabase(clusterFilePath);
    }

    public async init(): Promise<void> {

    }

    private static dbInstance: fdb.Database | null = null;

    public static getDatabase(clusterFilePath?: string): fdb.Database {
        if (!this.dbInstance) {
            fdb.setAPIVersion(FDB_API_VERSION);
            this.dbInstance = fdb.open(clusterFilePath); // Path to fdb.cluster file
        }
        return this.dbInstance;
    }

    public static async doTransaction<T>(
        db: fdb.Database,
        action: (tr: fdb.Transaction) => Promise<T>
    ): Promise<T> {
        return db.doTransaction(action);
    }
}
