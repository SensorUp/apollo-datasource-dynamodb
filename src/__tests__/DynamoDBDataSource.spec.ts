import { ApolloError } from 'apollo-server-errors';
import { DataSourceConfig } from 'apollo-datasource';

import { DynamoDBClientConfig, CreateTableCommandInput } from '@aws-sdk/client-dynamodb';
import {
  GetCommand,
  DeleteCommand,
  PutCommand,
  GetCommandInput,
  ScanCommandInput,
  QueryCommandInput,
  UpdateCommandInput,
  DeleteCommandInput,
} from '@aws-sdk/lib-dynamodb';

import { DynamoDBDataSource } from '../DynamoDBDataSource';
import { CACHE_PREFIX_KEY } from '../DynamoDBCache';
import { buildItemsCacheMap } from '../utils';
import { CacheKeyItemMap, ItemsList } from '../types';

const { MOCK_DYNAMODB_ENDPOINT } = process.env;

interface TestHashOnlyItem {
  id: string;
  test: string;
}

class TestHashOnly extends DynamoDBDataSource<TestHashOnlyItem> {
  constructor(tableName: string, tableKeySchema: CreateTableCommandInput['KeySchema'], config?: DynamoDBClientConfig) {
    super(tableName, tableKeySchema, config);
  }

  initialize(config: DataSourceConfig<Record<string, never>>): void {
    super.initialize(config);
  }
}

const keySchema: CreateTableCommandInput['KeySchema'] = [
  {
    AttributeName: 'id',
    KeyType: 'HASH',
  },
];
const testHashOnly = new TestHashOnly('test_hash_only', keySchema, {
  region: 'local',
  endpoint: MOCK_DYNAMODB_ENDPOINT,
  tls: false,
});
testHashOnly.initialize({ context: {}, cache: null });

const testHashOnlyItem: TestHashOnlyItem = {
  id: 'testId',
  test: 'testing',
};
const items: TestHashOnlyItem[] = [testHashOnlyItem];

beforeAll(async () => {
  await testHashOnly.dynamoDbDocClient.send(
    new PutCommand({
      TableName: testHashOnly.tableName,
      Item: testHashOnlyItem,
    })
  );
});

afterAll(async () => {
  await testHashOnly.dynamoDbDocClient.send(
    new DeleteCommand({
      TableName: testHashOnly.tableName,
      Key: { id: 'testId' },
    })
  );
});

describe('DynamoDBDataSource', () => {
  it('initializes a new TestHashOnly and instantiates props', () => {
    expect(testHashOnly.dynamoDbDocClient).toBeDefined();
    expect(testHashOnly.tableName).toBeDefined();
    expect(testHashOnly.tableKeySchema).toBeDefined();
    expect(testHashOnly.dynamodbCache).toBeDefined();
  });

  describe('getItem', () => {
    const dynamodbCacheGetItemMock = jest.spyOn(testHashOnly.dynamodbCache, 'getItem');

    afterEach(() => {
      dynamodbCacheGetItemMock.mockReset();
    });
    afterAll(() => {
      dynamodbCacheGetItemMock.mockRestore();
    });

    it('should return a TestHashOnly item', async () => {
      const getItemInput: GetCommandInput = {
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId',
        },
      };

      dynamodbCacheGetItemMock.mockResolvedValueOnce(testHashOnlyItem);

      const actual = await testHashOnly.getItem(getItemInput);

      expect(actual).toEqual(testHashOnlyItem);
      expect(dynamodbCacheGetItemMock).toBeCalledWith(getItemInput, undefined);
    });

    it('should throw an ApolloError if an issue occurs retrieving the record', async () => {
      const getItemInput: GetCommandInput = {
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId',
        },
      };

      dynamodbCacheGetItemMock.mockRejectedValueOnce(new ApolloError('Error'));

      await expect(testHashOnly.getItem(getItemInput)).rejects.toThrowError(new ApolloError('Error'));
      expect(dynamodbCacheGetItemMock).toBeCalledWith(getItemInput, undefined);
    });
  });

  const dynamodbCacheSetItemsInCacheMock = jest.spyOn(testHashOnly.dynamodbCache, 'setItemsInCache');
  const dynamodbCacheSetInCacheMock = jest.spyOn(testHashOnly.dynamodbCache, 'setInCache');

  afterEach(() => {
    dynamodbCacheSetItemsInCacheMock.mockReset();
    dynamodbCacheSetInCacheMock.mockReset();
  });
  afterAll(() => {
    dynamodbCacheSetItemsInCacheMock.mockRestore();
    dynamodbCacheSetInCacheMock.mockRestore();
  });

  it('query should return a list of TestHashOnlyItem records and add items to the cache', async () => {
    const queryInput: QueryCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: {
        ':id': 'testId',
      },
    };
    const ttl = 30;

    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: testHashOnlyItem,
      })
    );

    dynamodbCacheSetItemsInCacheMock.mockResolvedValueOnce();

    const actual: TestHashOnlyItem[] = await testHashOnly.query(queryInput, ttl);
    const cacheKeyItemMap: CacheKeyItemMap<TestHashOnlyItem> = buildItemsCacheMap(
      CACHE_PREFIX_KEY,
      testHashOnly.tableName,
      testHashOnly.tableKeySchema,
      actual
    );

    expect(actual).toEqual(items);
    expect(dynamodbCacheSetItemsInCacheMock).toBeCalledWith(cacheKeyItemMap, ttl);
  });

  it('query should return an empty list. setItemsInCache should not be invoked', async () => {
    const queryInput: QueryCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: {
        ':id': 'testId',
      },
    };
    const ttl = 30;

    const actual: TestHashOnlyItem[] = await testHashOnly.query(queryInput, ttl);

    expect(actual).toEqual([]);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });
  it('queryDetails should return an empty list. setItemsInCache should not be invoked', async () => {
    const queryInput: QueryCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: {
        ':id': 'testId',
      },
    };
    const ttl = 30;

    const actual: ItemsList<TestHashOnlyItem> = await testHashOnly.queryDetails(queryInput, ttl);

    expect(actual.items).toEqual([]);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });

  it('query should return a list of TestHashOnlyItem records but not add items to cache because of no ttl', async () => {
    const queryInput: QueryCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: {
        ':id': 'testId',
      },
    };

    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: testHashOnlyItem,
      })
    );

    const actual: TestHashOnlyItem[] = await testHashOnly.query(queryInput);

    expect(actual).toEqual(items);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });

  it('scan should return a list of TestHashOnlyItem records and add items to the cache', async () => {
    const scanInput: ScanCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
    };
    const ttl = 30;

    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: testHashOnlyItem,
      })
    );

    dynamodbCacheSetItemsInCacheMock.mockResolvedValueOnce();

    const actual: TestHashOnlyItem[] = await testHashOnly.scan(scanInput, ttl);
    const cacheKeyItemMap: CacheKeyItemMap<TestHashOnlyItem> = buildItemsCacheMap(
      CACHE_PREFIX_KEY,
      testHashOnly.tableName,
      testHashOnly.tableKeySchema,
      actual
    );

    expect(actual).toEqual(items);
    expect(dynamodbCacheSetItemsInCacheMock).toBeCalledWith(cacheKeyItemMap, ttl);
  });

  it('scan should return an empty list. setItemsInCache should not be invoked', async () => {
    const scanInput: ScanCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
    };
    const ttl = 30;

    const actual: TestHashOnlyItem[] = await testHashOnly.scan(scanInput, ttl);

    expect(actual).toEqual([]);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });
  it('scanDetails should return an empty list. setItemsInCache should not be invoked', async () => {
    const scanInput: ScanCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
    };
    const ttl = 30;

    const actual: ItemsList<TestHashOnlyItem> = await testHashOnly.scanDetails(scanInput, ttl);

    expect(actual.items).toEqual([]);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });

  it('scan should return a list of TestHashOnlyItem records but not add items to cache because of no ttl', async () => {
    const scanInput: ScanCommandInput = {
      TableName: testHashOnly.tableName,
      ConsistentRead: true,
    };

    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: testHashOnlyItem,
      })
    );

    const actual: TestHashOnlyItem[] = await testHashOnly.scan(scanInput);

    expect(actual).toEqual(items);
    expect(dynamodbCacheSetItemsInCacheMock).not.toBeCalled();
  });

  it('should put the item and store it in the cache', async () => {
    const item2: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing2',
    };
    const ttl = 30;
    const cacheKey = `${CACHE_PREFIX_KEY}${testHashOnly.tableName}:id-testId2`;

    dynamodbCacheSetInCacheMock.mockResolvedValueOnce();

    const actual: TestHashOnlyItem = await testHashOnly.put(item2, ttl);
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId2',
        },
      })
    );

    expect(actual).toEqual(item2);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).toBeCalledWith(cacheKey, actual, ttl);

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId2' },
      })
    );
  });

  it('should put the item and not store it in the cache because the ttl is null', async () => {
    const item3: TestHashOnlyItem = {
      id: 'testId3',
      test: 'testing3',
    };

    const actual: TestHashOnlyItem = await testHashOnly.put(item3);
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId3',
        },
      })
    );

    expect(actual).toEqual(item3);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).not.toBeCalled();

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId3' },
      })
    );
  });

  it('should update the item in the table and store it in the cache', async () => {
    const item2: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing2',
    };
    const itemUpdated: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing_updated',
    };
    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: item2,
      })
    );

    const givenKey: UpdateCommandInput['Key'] = { id: 'testId2' } as never;
    const givenUpdateExpression: UpdateCommandInput['UpdateExpression'] = 'SET #test = :test';
    const givenExpressionAttributeNames: UpdateCommandInput['ExpressionAttributeNames'] = { '#test': 'test' };
    const givenExpressionAttributeValues: UpdateCommandInput['ExpressionAttributeValues'] = {
      ':test': 'testing_updated',
    };
    const ttl = 30;
    const cacheKey = `${CACHE_PREFIX_KEY}${testHashOnly.tableName}:id-testId2`;

    dynamodbCacheSetInCacheMock.mockResolvedValueOnce();

    const actual = await testHashOnly.update(
      givenKey,
      givenUpdateExpression,
      givenExpressionAttributeNames,
      givenExpressionAttributeValues,
      ttl
    );
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId2',
        },
      })
    );

    expect(actual).toEqual(itemUpdated);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).toBeCalledWith(cacheKey, actual, ttl);

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId2' },
      })
    );
  });

  it('should update the item in the table and not set the item in the cache - no ttl passed in', async () => {
    const item2: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing2',
    };
    const itemUpdated: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing_updated',
    };
    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: item2,
      })
    );

    const givenKey: UpdateCommandInput['Key'] = { id: 'testId2' } as never;
    const givenUpdateExpression: UpdateCommandInput['UpdateExpression'] = 'SET #test = :test';
    const givenExpressionAttributeNames: UpdateCommandInput['ExpressionAttributeNames'] = { '#test': 'test' };
    const givenExpressionAttributeValues: UpdateCommandInput['ExpressionAttributeValues'] = {
      ':test': 'testing_updated',
    };

    const actual = await testHashOnly.update(
      givenKey,
      givenUpdateExpression,
      givenExpressionAttributeNames,
      givenExpressionAttributeValues
    );
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId2',
        },
      })
    );

    expect(actual).toEqual(itemUpdated);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).not.toBeCalled();

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId2' },
      })
    );
  });

  it('should updateConditional the item in the table and store it in the cache', async () => {
    const item2: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing2',
    };
    const itemUpdated: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing_updated',
    };
    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: item2,
      })
    );

    const givenKey: UpdateCommandInput['Key'] = { id: 'testId2' } as never;
    const givenUpdateExpression: UpdateCommandInput['UpdateExpression'] = 'SET #test = :test';
    const givenConditionalExpression: UpdateCommandInput['ConditionExpression'] = '#test <> :test';
    const givenExpressionAttributeNames: UpdateCommandInput['ExpressionAttributeNames'] = { '#test': 'test' };
    const givenExpressionAttributeValues: UpdateCommandInput['ExpressionAttributeValues'] = {
      ':test': 'testing_updated',
    };

    const ttl = 30;
    const cacheKey = `${CACHE_PREFIX_KEY}${testHashOnly.tableName}:id-testId2`;

    dynamodbCacheSetInCacheMock.mockResolvedValueOnce();

    const actual = await testHashOnly.updateConditional(
      givenKey,
      givenUpdateExpression,
      givenConditionalExpression,
      givenExpressionAttributeNames,
      givenExpressionAttributeValues,
      ttl
    );
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId2',
        },
      })
    );

    expect(actual).toEqual(itemUpdated);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).toBeCalledWith(cacheKey, actual, ttl);

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId2' },
      })
    );
  });

  it('should updateConditional the item in the table and not set the item in the cache - no ttl passed in', async () => {
    const item2: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing2',
    };
    const itemUpdated: TestHashOnlyItem = {
      id: 'testId2',
      test: 'testing_updated',
    };
    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: item2,
      })
    );

    const givenKey: UpdateCommandInput['Key'] = { id: 'testId2' } as never;
    const givenUpdateExpression: UpdateCommandInput['UpdateExpression'] = 'SET #test = :test';
    const givenConditionalExpression: UpdateCommandInput['ConditionExpression'] = '#test <> :test';
    const givenExpressionAttributeNames: UpdateCommandInput['ExpressionAttributeNames'] = { '#test': 'test' };
    const givenExpressionAttributeValues: UpdateCommandInput['ExpressionAttributeValues'] = {
      ':test': 'testing_updated',
    };

    const actual = await testHashOnly.updateConditional(
      givenKey,
      givenUpdateExpression,
      givenConditionalExpression,
      givenExpressionAttributeNames,
      givenExpressionAttributeValues
    );
    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'testId2',
        },
      })
    );

    expect(actual).toEqual(itemUpdated);
    expect(Item).toBeDefined();
    expect(actual).toEqual(Item);
    expect(dynamodbCacheSetInCacheMock).not.toBeCalled();

    await testHashOnly.dynamoDbDocClient.send(
      new DeleteCommand({
        TableName: testHashOnly.tableName,
        Key: { id: 'testId2' },
      })
    );
  });

  it('should delete the item from the table', async () => {
    const dynamodbCacheRemoveItemFromCacheMock = jest.spyOn(testHashOnly.dynamodbCache, 'removeItemFromCache');

    const itemToDelete: TestHashOnlyItem = {
      id: 'delete_me',
      test: 'gonna be deleted',
    };
    await testHashOnly.dynamoDbDocClient.send(
      new PutCommand({
        TableName: testHashOnly.tableName,
        Item: itemToDelete,
      })
    );

    const givenKey: DeleteCommandInput['Key'] = { id: 'delete_me' } as never;

    dynamodbCacheRemoveItemFromCacheMock.mockResolvedValueOnce();

    await testHashOnly.delete(givenKey);

    const { Item } = await testHashOnly.dynamoDbDocClient.send(
      new GetCommand({
        TableName: testHashOnly.tableName,
        ConsistentRead: true,
        Key: {
          id: 'delete_me',
        },
      })
    );

    expect(Item).not.toBeDefined();
    expect(dynamodbCacheRemoveItemFromCacheMock).toBeCalledWith(testHashOnly.tableName, givenKey);
  });
});
