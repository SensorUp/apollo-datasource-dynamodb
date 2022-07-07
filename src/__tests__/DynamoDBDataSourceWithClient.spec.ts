import { DataSourceConfig } from 'apollo-datasource';

import { DynamoDBClient, CreateTableCommandInput } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

import { DynamoDBDataSource } from '../DynamoDBDataSource';

interface TestItem {
  id: string;
  item1: string;
  item2: string;
}

class TestWithClient extends DynamoDBDataSource<TestItem> {
  constructor(tableName: string, tableKeySchema: CreateTableCommandInput['KeySchema'], client: DynamoDBDocumentClient) {
    super(tableName, tableKeySchema, null, client);
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

// const dynamodbMock = mockClient(DynamoDBClient)
const dynamoDbClient = new DynamoDBClient({});
const client = DynamoDBDocumentClient.from(dynamoDbClient);

const testWithClient = new TestWithClient('test_with_client', keySchema, client);
testWithClient.initialize({ context: {}, cache: null });

// const testItem: TestItem = {
// id: 'testWithClientId',
// item1: 'testing1',
// item2: 'testing2',
// };

beforeAll(async () => {
  // await testWithClient.dynamoDbDocClient.send(
  //  new PutCommand({
  //    TableName: testWithClient.tableName,
  //    Item: testItem,
  //  })
  // );
});

afterAll(async () => {
  // await testWithClient.dynamoDbDocClient.send(
  //  new DeleteCommand({
  //    TableName: testWithClient.tableName,
  //    Key: { id: 'testWithClientId' },
  //  })
  // );
});

describe('DynamoDBDataSource With Initialized Client', () => {
  it('initializes a new TestHashOnly and instantiates props', () => {
    expect(testWithClient.dynamoDbDocClient).toBeDefined();
    expect(testWithClient.dynamoDbDocClient).toEqual(client);
    expect(testWithClient.tableName).toBeDefined();
    expect(testWithClient.tableKeySchema).toBeDefined();
    expect(testWithClient.dynamodbCache).toBeDefined();
  });
});
