import { ApolloError } from 'apollo-server-errors';
import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, GetCommandInput } from '@aws-sdk/lib-dynamodb';

import { DynamoDBCacheImpl, CACHE_PREFIX_KEY, TTL_SEC } from '../DynamoDBCache';
import { buildCacheKey } from '../utils';

const dynamodbMock = mockClient(DynamoDBDocumentClient);

interface TestHashOnlyItem {
  id: string;
  test: string;
}

describe('DynamoDBCache', () => {
  const dynamoDbClient = new DynamoDBClient({});
  const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoDbClient);

  describe('retrieveFromCache', () => {
    const dynamodbCache = new DynamoDBCacheImpl(dynamoDbDocumentClient);
    const getFromCacheMock = jest.spyOn(dynamodbCache.keyValueCache, 'get');

    afterEach(() => {
      getFromCacheMock.mockReset();
    });
    afterAll(() => {
      getFromCacheMock.mockRestore();
    });

    it('should return undefined if no item is found in the cache', async () => {
      const given = `${CACHE_PREFIX_KEY}test:id-testId`;

      getFromCacheMock.mockResolvedValueOnce(undefined);

      const actual = await dynamodbCache.retrieveFromCache(given);

      expect(actual).toEqual(undefined);
      expect(getFromCacheMock).toBeCalledWith(given);
    });

    it('should return undefined if an error is thrown retrieving an item from the cache', async () => {
      const given = `${CACHE_PREFIX_KEY}test:id-testId`;

      getFromCacheMock.mockRejectedValueOnce(new Error('Error retrieving item from cache'));

      const actual = await dynamodbCache.retrieveFromCache(given);

      expect(actual).toEqual(undefined);
      expect(getFromCacheMock).toBeCalledWith(given);
    });

    it('should return the parsed item from the cache', async () => {
      const given = `${CACHE_PREFIX_KEY}test:id-testId`;
      const expected = {
        id: 'testId',
        test: 'test',
      };
      const itemFromCache = JSON.stringify(expected);

      getFromCacheMock.mockResolvedValueOnce(itemFromCache);

      const actual = await dynamodbCache.retrieveFromCache(given);

      expect(actual).toEqual(expected);
      expect(getFromCacheMock).toBeCalledWith(given);
    });
  });

  describe('setInCache', () => {
    const dynamodbCache = new DynamoDBCacheImpl(dynamoDbDocumentClient);
    const setInCacheMock = jest.spyOn(dynamodbCache.keyValueCache, 'set');

    afterEach(() => {
      setInCacheMock.mockReset();
    });
    afterAll(() => {
      setInCacheMock.mockRestore();
    });

    it('should set the item in the cache with default TTL', async () => {
      const givenKey = `${CACHE_PREFIX_KEY}test:id-testId`;
      const givenItem = {
        id: 'testId',
        test: 'test',
      };

      setInCacheMock.mockResolvedValueOnce();

      await dynamodbCache.setInCache(givenKey, givenItem);

      expect(setInCacheMock).toBeCalledWith(givenKey, JSON.stringify(givenItem), { ttl: TTL_SEC });
    });
  });

  describe('setItemsInCache', () => {
    const dynamodbCache = new DynamoDBCacheImpl(dynamoDbDocumentClient);
    const setInCacheMock = jest.spyOn(dynamodbCache, 'setInCache');

    afterEach(() => {
      setInCacheMock.mockReset();
    });
    afterAll(() => {
      setInCacheMock.mockRestore();
    });

    it('should set the item in the cache with default TTL', async () => {
      const givenKey = `${CACHE_PREFIX_KEY}test:id-testId`;
      const givenItem = {
        id: 'testId',
        test: 'test',
      };
      const given: { [cacheKey: string]: TestHashOnlyItem } = {
        [givenKey]: givenItem,
      };

      setInCacheMock.mockResolvedValueOnce();

      await dynamodbCache.setItemsInCache(given);

      expect(setInCacheMock).toBeCalledTimes(Object.keys(given).length);
      expect(setInCacheMock).toBeCalledWith(givenKey, givenItem, TTL_SEC);
    });
  });

  describe('getItem', () => {
    const dynamodbCache = new DynamoDBCacheImpl<TestHashOnlyItem>(dynamoDbDocumentClient);
    const testHashOnlyTableName = 'test_hash_only';
    const testHashOnlyItem: TestHashOnlyItem = { id: 'testId', test: 'testing' };
    const retrieveFromCacheMock = jest.spyOn(dynamodbCache, 'retrieveFromCache');
    const setInCacheMock = jest.spyOn(dynamodbCache, 'setInCache');

    afterEach(() => {
      retrieveFromCacheMock.mockReset();
      setInCacheMock.mockReset();
    });
    afterAll(async () => {
      retrieveFromCacheMock.mockRestore();
      setInCacheMock.mockRestore();
    });

    it('should return the item retrieved from the cache', async () => {
      const givenGetItemInput: GetCommandInput = {
        TableName: testHashOnlyTableName,
        ConsistentRead: true,
        Key: { id: 'testId' },
      };
      const givenTtl = TTL_SEC;
      const expected = testHashOnlyItem;
      const cacheKey = buildCacheKey(CACHE_PREFIX_KEY, testHashOnlyTableName, { id: 'testId' } as any);

      retrieveFromCacheMock.mockResolvedValueOnce(expected);

      const actual: TestHashOnlyItem = await dynamodbCache.getItem(givenGetItemInput, givenTtl);

      expect(actual).toEqual(expected);
      expect(retrieveFromCacheMock).toBeCalledWith(cacheKey);
      expect(setInCacheMock).not.toBeCalled();
    });

    it('should return the item retrieved from the DynamoDB table and set in the cache', async () => {
      const givenGetItemInput: GetCommandInput = {
        TableName: testHashOnlyTableName,
        ConsistentRead: true,
        Key: { id: 'testId' },
      };
      const givenTtl = TTL_SEC;

      dynamodbMock.on(GetCommand, givenGetItemInput).resolves({ Item: testHashOnlyItem });
      //    await dynamodbCache.docClient.send(
      //      new PutCommand({
      //        TableName: testHashOnlyTableName,
      //        Item: testHashOnlyItem,
      //      })
      //    );
      const cacheKey = buildCacheKey(CACHE_PREFIX_KEY, testHashOnlyTableName, { id: 'testId' } as any);

      retrieveFromCacheMock.mockResolvedValueOnce(undefined);
      setInCacheMock.mockResolvedValueOnce();

      const actual: TestHashOnlyItem = await dynamodbCache.getItem(givenGetItemInput, givenTtl);

      expect(actual).toBeDefined();
      expect(actual).toEqual(testHashOnlyItem);
      expect(retrieveFromCacheMock).toBeCalledWith(cacheKey);
      expect(setInCacheMock).toBeCalledWith(cacheKey, actual, givenTtl);

      //    await dynamodbCache.docClient.send(
      //      new DeleteCommand({
      //        TableName: testHashOnlyTableName,
      //        Key: { id: 'testId' },
      //      })
      //    );
    });

    it('should return an ApolloError if an error is thrown retrieving the record', async () => {
      const givenGetItemInput: GetCommandInput = {
        TableName: testHashOnlyTableName,
        ConsistentRead: true,
        Key: { id: 'testId' },
      };
      const givenTtl = TTL_SEC;
      const cacheKey = buildCacheKey(CACHE_PREFIX_KEY, testHashOnlyTableName, { id: 'testId' } as any);
      const error = new Error('Error setting item in cache');

      retrieveFromCacheMock.mockRejectedValueOnce(error);

      await expect(dynamodbCache.getItem(givenGetItemInput, givenTtl)).rejects.toThrowError(
        new ApolloError('Error setting item in cache')
      );
      expect(retrieveFromCacheMock).toBeCalledWith(cacheKey);
      expect(setInCacheMock).not.toBeCalled();
    });

    it('should return an ApolloError if no record is found', async () => {
      const givenGetItemInput: GetCommandInput = {
        TableName: testHashOnlyTableName,
        ConsistentRead: true,
        Key: { id: 'does_not_exist' },
      };
      const givenTtl = TTL_SEC;
      const cacheKey = buildCacheKey(CACHE_PREFIX_KEY, testHashOnlyTableName, { id: 'does_not_exist' } as any);

      retrieveFromCacheMock.mockRejectedValueOnce(undefined);

      await expect(dynamodbCache.getItem(givenGetItemInput, givenTtl)).rejects.toThrowError(
        new ApolloError('An error occurred attempting to retrieve the item')
      );
      expect(retrieveFromCacheMock).toBeCalledWith(cacheKey);
      expect(setInCacheMock).not.toBeCalled();
    });
  });

  describe('removeItemFromCache', () => {
    const dynamodbCache = new DynamoDBCacheImpl<TestHashOnlyItem>(dynamoDbDocumentClient);
    const deleteFromCacheMock = jest.spyOn(dynamodbCache.keyValueCache, 'delete');

    afterEach(() => {
      deleteFromCacheMock.mockReset();
    });
    afterAll(() => {
      deleteFromCacheMock.mockRestore();
    });

    it('should remove the item from the cache and return true', async () => {
      const givenTableName = 'test';
      const givenKey: GetCommandInput['Key'] = { id: 'testId' };
      const givenCacheKey = `${CACHE_PREFIX_KEY}test:id-testId`;

      deleteFromCacheMock.mockResolvedValueOnce(true);

      const actual = await dynamodbCache.removeItemFromCache(givenTableName, givenKey);

      expect(actual).toEqual(true);
      expect(deleteFromCacheMock).toBeCalledWith(givenCacheKey);
    });

    it('should throw an ApolloError if the dynamodbCache.keyValueCache.delete throws an error - with given error message', async () => {
      const givenTableName = 'test';
      const givenKey: GetCommandInput['Key'] = { id: 'testId' };
      const givenCacheKey = `${CACHE_PREFIX_KEY}test:id-testId`;

      deleteFromCacheMock.mockRejectedValueOnce(new Error('Error'));

      await expect(dynamodbCache.removeItemFromCache(givenTableName, givenKey)).rejects.toThrowError(
        new ApolloError('Error')
      );
      expect(deleteFromCacheMock).toBeCalledWith(givenCacheKey);
    });

    it('should throw an ApolloError if the dynamodbCache.keyValueCache.delete throws an error - with default error message', async () => {
      const givenTableName = 'test';
      const givenKey: GetCommandInput['Key'] = { id: 'testId' };
      const givenCacheKey = `${CACHE_PREFIX_KEY}test:id-testId`;

      deleteFromCacheMock.mockRejectedValueOnce(null);

      await expect(dynamodbCache.removeItemFromCache(givenTableName, givenKey)).rejects.toThrowError(
        new ApolloError('An error occurred trying to evict the item from the cache')
      );
      expect(deleteFromCacheMock).toBeCalledWith(givenCacheKey);
    });
  });
});
