import { FDBController } from './core/fdbController';
import { Subspace, directory as fdbDirectory, Database, Transaction } from 'foundationdb';
import {
    AttributeValue,
    Item,
    KeySchemaElement,
    AttributeDefinition,
    TableDescription,
    GetItemInput,
    GetItemOutput
} from './types/dynamodbTypes'; // These types are generic to DynamoDB's structure
import {
    serializeItem,
    deserializeItem,
    encodeFDBKey,
    // extractScalarValue, // Now part of ConcreteDB class or specific actions
    DYNAMO_LAYER_PREFIX, // Consider renaming if it's specific to ConcreteDB's layer
    TABLE_METADATA_DIR_NAME,
    TABLE_DATA_DIR_NAME
} from './utils';
import { DynamoDBOperationError, ResourceNotFoundException, ValidationException, ResourceInUseException } from './errors/dynamoErrors'; // Error names can remain generic or be prefixed if desired


export class ConcreteDB { // Renamed class
  private db: Database;
  private rootDirectory: Subspace | null = null;
  private tableMetadataDir: Subspace | null = null;
  private tableDataDir: Subspace | null = null;

  constructor(clusterFilePath?: string) {
    this.db = FDBController.getDatabase(clusterFilePath);
  }

  public async init(): Promise<void> {
    try {
        // You might choose a more specific prefix for ConcreteDB if DYNAMO_LAYER_PREFIX was generic
        const concreteDBLayerPrefix = Buffer.from([0xCD, 0xDB]); // Example for ConcreteDB
        this.rootDirectory = await fdbDirectory.createOrOpen(this.db, concreteDBLayerPrefix);
        this.tableMetadataDir = await this.rootDirectory.createOrOpen(this.db, [TABLE_METADATA_DIR_NAME]);
        this.tableDataDir = await this.rootDirectory.createOrOpen(this.db, [TABLE_DATA_DIR_NAME]);
        console.log('ConcreteDB Layer initialized with directories.');
    } catch (error) {
        console.error("Failed to initialize ConcreteDB directories:", error);
        throw error; // Or a custom ConcreteDBError
    }
  }

  private getTableDataSubspaceSync(tableId: string): Subspace {
    if (!this.tableDataDir) {
        throw new Error("Table data directory not initialized. Call init() first.");
    }
    return this.tableDataDir.subspace([tableId]);
  }

  private async getTableConfig(tr: Transaction, tableName: string): Promise<TableDescription | null> {
    if (!this.tableMetadataDir) {
        throw new Error("Table metadata directory not initialized. Call init() first.");
    }
    const tableConfigKey = this.tableMetadataDir.pack([tableName]);
    const configBuffer = await tr.get(tableConfigKey);
    if (!configBuffer || configBuffer.length === 0) {
      return null;
    }
    return JSON.parse(configBuffer.toString()) as TableDescription;
  }

   private extractScalarValue(attrValue: AttributeValue, attrName: string, attrDefType: 'S' | 'N' | 'B'): string | number | Buffer {
      if ('S' in attrValue && attrDefType === 'S') return attrValue.S;
      if ('N' in attrValue && attrDefType === 'N') return parseFloat(attrValue.N);
      if ('B' in attrValue && attrDefType === 'B') return attrValue.B;
      throw new ValidationException(`Type mismatch or unsupported type for key attribute ${attrName}. Expected ${attrDefType}.`);
  }

  // --- Core Actions ---

  async createTable(params: {
    TableName: string;
    KeySchema: KeySchemaElement[];
    AttributeDefinitions: AttributeDefinition[];
  }): Promise<{ TableDescription?: TableDescription }> {
    if (!this.tableMetadataDir) throw new DynamoDBOperationError("ConcreteDB Layer not initialized. Call init().");
    const { TableName, KeySchema, AttributeDefinitions } = params;

    if (!TableName || !/^[a-zA-Z0-9_.-]{3,255}$/.test(TableName)) {
        throw new ValidationException("Invalid TableName format or length.");
    }
    if (!KeySchema || KeySchema.length === 0 || KeySchema.filter(k => k.KeyType === 'HASH').length !== 1) {
        throw new ValidationException("KeySchema must contain exactly one HASH key.");
    }
    if (KeySchema.filter(k => k.KeyType === 'RANGE').length > 1) {
        throw new ValidationException("KeySchema can contain at most one RANGE key.");
    }
    if(!AttributeDefinitions || AttributeDefinitions.length === 0) {
        throw new ValidationException("AttributeDefinitions must be provided.");
    }
    for (const ksElement of KeySchema) {
        if (!AttributeDefinitions.find(ad => ad.AttributeName === ksElement.AttributeName)) {
            throw new ValidationException(`KeySchema attribute ${ksElement.AttributeName} not found in AttributeDefinitions.`);
        }
    }


    const tableId = TableName; // Keep simple for now, UUID in prod
    const newTableDescription: TableDescription = {
      TableName,
      TableId: tableId,
      KeySchema,
      AttributeDefinitions,
      TableStatus: 'CREATING',
      CreationDateTime: Math.floor(Date.now() / 1000),
    };

    try {
        await FDBController.doTransaction(this.db, async (tr) => {
            const existingConfig = await this.getTableConfig(tr, TableName);
            if (existingConfig) {
                throw new ResourceInUseException(`Table ${TableName} already exists.`);
            }
            const tableConfigKey = this.tableMetadataDir!.pack([TableName]);
            tr.set(tableConfigKey, Buffer.from(JSON.stringify(newTableDescription)));
        });

        newTableDescription.TableStatus = 'ACTIVE';
        await FDBController.doTransaction(this.db, async (tr) => {
             const tableConfigKey = this.tableMetadataDir!.pack([TableName]);
            tr.set(tableConfigKey, Buffer.from(JSON.stringify(newTableDescription)));
        });
        return { TableDescription: newTableDescription };
    } catch(error: any) {
        if (error instanceof DynamoDBOperationError) throw error; // Keep specific errors
        throw new DynamoDBOperationError(`CreateTable failed for ${TableName}: ${error.message}`);
    }
  }

  async getItem(params: GetItemInput): Promise<GetItemOutput> {
    if (!this.tableDataDir || !this.tableMetadataDir) {
        throw new DynamoDBOperationError("ConcreteDB Layer not initialized. Call init().");
    }
    const { TableName, Key } = params;

    return FDBController.doTransaction(this.db, async (tr) => {
        const tableConfig = await this.getTableConfig(tr, TableName);
        if (!tableConfig || tableConfig.TableStatus !== 'ACTIVE') {
          throw new ResourceNotFoundException(`Table ${TableName} not found or not active.`);
        }

        const tableDataSubspace = this.getTableDataSubspaceSync(tableConfig.TableId);
        const fdbKey = encodeFDBKey(tableConfig.KeySchema, Key, tableDataSubspace, tableConfig.AttributeDefinitions);

        const itemBuffer = await tr.get(fdbKey);
        const item = deserializeItem(itemBuffer);

        return item ? { Item: item } : {};
    });
  }

  // ... (Implement other core methods: PutItem, DeleteItem, ListTables, DescribeTable etc.)
  // ... Ensure to use `this.extractScalarValue` and other helpers consistently.
}

// Example of how it might be used:
// async function run() {
//   const dbInstance = new ConcreteDB(); // Renamed
//   await dbInstance.init();
//   console.log("ConcreteDB instance initialized.");
//   // ... use dbInstance
// }
// run().catch(console.error);
