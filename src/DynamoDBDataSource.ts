import { DataSource, DataSourceConfig } from 'apollo-datasource';

import { Agent } from 'https';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { captureAWSv3Client } from 'aws-xray-sdk-core';

import {
  DynamoDBClient,
  DynamoDBClientConfig,
  CreateTableCommandInput,
  GetItemCommandInput,
  PutItemCommandInput,
  UpdateItemCommandInput,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  ScanCommand,
  QueryCommand,
  DeleteCommand,
  UpdateCommand,
  DeleteCommandInput,
  GetCommandInput,
  PutCommandInput,
  QueryCommandInput,
  ScanCommandInput,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb';

import { DynamoDBCache, DynamoDBCacheImpl, CACHE_PREFIX_KEY } from './DynamoDBCache';
import { buildItemsCacheMap, buildCacheKey, buildKey } from './utils';
import { CacheKeyItemMap, ItemsDetails, ItemsList } from './types';

const awsClientDefaultOptions = {
  apiVersion: 'latest',
  requestHandler: new NodeHttpHandler({
    httpsAgent: new Agent({
      keepAlive: true,
      secureProtocol: 'TLSv1_2_method',
    }),
  }),
};

/**
 * Data Source to interact with DynamoDB.
 * @param ITEM the type of the item to retrieve from the DynamoDB table
 */
export abstract class DynamoDBDataSource<ITEM = unknown, TContext = unknown> extends DataSource {
  readonly dynamoDbDocClient: DynamoDBDocumentClient;
  readonly tableName!: string;
  readonly tableKeySchema!: CreateTableCommandInput['KeySchema'];
  dynamodbCache!: DynamoDBCache<ITEM>;
  context!: TContext;

  itemsDetails: ItemsDetails;

  /**
   * Create a `DynamoDBDataSource` instance with the supplied params
   * @param tableName the name of the DynamoDB table the class will be interacting with
   * @param tableKeySchema the key structure schema of the table
   * @param config an optional DynamoDBClientConfig object to use in building the DynamoDB.DocumentClient
   * @param client an optional initialized DynamoDB.Document client instance to use to set the client in the class instance
   */
  constructor(
    tableName: string,
    tableKeySchema: CreateTableCommandInput['KeySchema'],
    config?: DynamoDBClientConfig,
    client?: DynamoDBDocumentClient
  ) {
    super();
    this.tableName = tableName;
    this.tableKeySchema = tableKeySchema;
    if (client != null) {
      this.dynamoDbDocClient = client;
    } else {
      const dynamoDbClient = captureAWSv3Client(new DynamoDBClient({ ...awsClientDefaultOptions, ...config }));
      this.dynamoDbDocClient = DynamoDBDocumentClient.from(dynamoDbClient);
    }
  }

  initialize({ context, cache }: DataSourceConfig<TContext>): void {
    this.context = context;
    this.dynamodbCache = new DynamoDBCacheImpl(this.dynamoDbDocClient, cache);
  }

  /**
   * Retrieve the item with the given `GetItemInput`.
   * - Attempt to retrieve the item from the cache.
   * - If the item does not exist in the cache, retrieve the item from the table, then add the item to the cache
   * @param getItemInput the input that provides information about which record to retrieve from the cache/dynamodb table
   * @param ttl the time-to-live value of the item in the cache. determines how long the item persists in the cache
   */
  async getItem(getItemInput: GetCommandInput, ttl?: number): Promise<ITEM> {
    return await this.dynamodbCache.getItem(getItemInput, ttl);
  }

  async cacheItems(items: ITEM[], ttl?: number): Promise<void> {
    // store the items in the cache
    if (items.length && ttl) {
      const cacheKeyItemMap: CacheKeyItemMap<ITEM> = buildItemsCacheMap(
        CACHE_PREFIX_KEY,
        this.tableName,
        this.tableKeySchema,
        items
      );
      await this.dynamodbCache.setItemsInCache(cacheKeyItemMap, ttl);
    }
  }

  /**
   * Query for a list of records by the given query input.
   * If the ttl has a value, and items are returned, store the items in the cache
   * @param queryInput the defined query that tells the document client which records to retrieve from the table
   * @param ttl the time-to-live value of the item in the cache. determines how long the item persists in the cache
   */
  async query(queryInput: QueryCommandInput, ttl?: number): Promise<ITEM[]> {
    const output = await this.dynamoDbDocClient.send(new QueryCommand(queryInput));
    const items: ITEM[] = output.Items as ITEM[];

    await this.cacheItems(items, ttl);

    return items;
  }

  /**
   * Query for a list of records by the given query input.
   * If the ttl has a value, and items are returned, store the items in the cache
   * @param queryInput the defined query that tells the document client which records to retrieve from the table
   * @param ttl the time-to-live value of the item in the cache. determines how long the item persists in the cache
   */
  async queryDetails(queryInput: QueryCommandInput, ttl?: number): Promise<ItemsList<ITEM>> {
    const output = await this.dynamoDbDocClient.send(new QueryCommand(queryInput));
    const items: ITEM[] = output.Items as ITEM[];
    const details: ItemsDetails = {
      Count: output.Count,
      ScannedCount: output.ScannedCount,
      LastEvaluatedKey: output.LastEvaluatedKey,
    } as ItemsDetails;

    await this.cacheItems(items, ttl);

    return { items, details };
  }

  /**
   * Scan for a list of records by the given scan input.
   * If the ttl has a value, and items are returned, store the items in the cache
   * @param scanInput the scan input that tell the document client how to scan for records in the table
   * @param ttl the time-to-live value of the item in the cache. determines how long the item persists in the cache
   */
  async scan(scanInput: ScanCommandInput, ttl?: number): Promise<ITEM[]> {
    const output = await this.dynamoDbDocClient.send(new ScanCommand(scanInput));
    const items: ITEM[] = output.Items as ITEM[];

    await this.cacheItems(items, ttl);

    return items;
  }
  async scanDetails(scanInput: ScanCommandInput, ttl?: number): Promise<ItemsList<ITEM>> {
    const output = await this.dynamoDbDocClient.send(new ScanCommand(scanInput));
    const items: ITEM[] = output.Items as ITEM[];
    const details: ItemsDetails = {
      Count: output.Count,
      ScannedCount: output.ScannedCount,
      LastEvaluatedKey: output.LastEvaluatedKey,
    } as ItemsDetails;

    await this.cacheItems(items, ttl);

    return { items, details };
  }

  /**
   * Store the item in the table and add the item to the cache
   * @param item the item to store in the table
   * @param ttl the time-to-live value of how long to persist the item in the cache
   */
  async put(item: ITEM, ttl?: number, conditionExpression?: PutItemCommandInput['ConditionExpression']): Promise<ITEM> {
    const putItemInput: PutCommandInput = {
      TableName: this.tableName,
      Item: item,
      ...(conditionExpression ? { ConditionExpression: conditionExpression } : {}),
    };
    await this.dynamoDbDocClient.send(new PutCommand(putItemInput));

    if (ttl) {
      const key: GetItemCommandInput['Key'] = buildKey(this.tableKeySchema, item);
      const cacheKey: string = buildCacheKey(CACHE_PREFIX_KEY, this.tableName, key);
      await this.dynamodbCache.setInCache(cacheKey, item, ttl);
    }

    return item;
  }

  /**
   * Update the item in the table and reset the item in the cache
   * @param key the key of the item in the table to update
   * @param ttl the time-to-live value of how long to persist the item in the cache
   */
  async update(
    key: UpdateItemCommandInput['Key'],
    updateExpression: UpdateItemCommandInput['UpdateExpression'],
    expressionAttributeNames: UpdateItemCommandInput['ExpressionAttributeNames'],
    expressionAttributeValues: UpdateItemCommandInput['ExpressionAttributeValues'],
    ttl?: number,
    conditionExpression?: UpdateItemCommandInput['ConditionExpression']
  ): Promise<ITEM> {
    const updateItemInput: UpdateCommandInput = {
      TableName: this.tableName,
      Key: key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ...(conditionExpression ? { ConditionExpression: conditionExpression } : {}),
    };
    const output = await this.dynamoDbDocClient.send(new UpdateCommand(updateItemInput));
    const updated: ITEM = output.Attributes as ITEM;

    if (updated && ttl) {
      const cacheKey: string = buildCacheKey(CACHE_PREFIX_KEY, this.tableName, key);
      await this.dynamodbCache.setInCache(cacheKey, updated, ttl);
    }

    return updated;
  }

  async updateConditional(
    key: UpdateItemCommandInput['Key'],
    updateExpression: UpdateItemCommandInput['UpdateExpression'],
    conditionExpression: UpdateItemCommandInput['ConditionExpression'],
    expressionAttributeNames: UpdateItemCommandInput['ExpressionAttributeNames'],
    expressionAttributeValues: UpdateItemCommandInput['ExpressionAttributeValues'],
    ttl?: number
  ): Promise<ITEM> {
    const updateItemInput: UpdateCommandInput = {
      TableName: this.tableName,
      Key: key,
      ReturnValues: 'ALL_NEW',
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ConditionExpression: conditionExpression,
    };
    const output = await this.dynamoDbDocClient.send(new UpdateCommand(updateItemInput));
    const updated: ITEM = output.Attributes as ITEM;

    if (updated && ttl) {
      const cacheKey: string = buildCacheKey(CACHE_PREFIX_KEY, this.tableName, key);
      await this.dynamodbCache.setInCache(cacheKey, updated, ttl);
    }

    return updated;
  }

  /**
   * Delete the given item from the table
   * @param key the key of the item to delete from the table
   */
  async delete(key: GetItemCommandInput['Key']): Promise<void> {
    const deleteItemInput: DeleteCommandInput = {
      TableName: this.tableName,
      Key: key,
    };

    await this.dynamoDbDocClient.send(new DeleteCommand(deleteItemInput));

    await this.dynamodbCache.removeItemFromCache(this.tableName, key);
  }
}
