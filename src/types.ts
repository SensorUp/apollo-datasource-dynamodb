import { DynamoDB } from 'aws-sdk';

export interface CacheKeyItemMap<T = unknown> {
  [key: string]: T;
}
export interface ItemsList<T = unknown> {
  items: T[];
  details: ItemsDetails;
}

export interface ItemsDetails {
  /**
   * The number of items in the response. If you used a QueryFilter in the request, then Count is the number of items returned after the filter was applied, and ScannedCount is the number of matching items before the filter was applied. If you did not use a filter in the request, then Count and ScannedCount are the same.
   */
  Count?: DynamoDB.DocumentClient.Integer;
  /**
   * The number of items evaluated, before any ScanFilter is applied. A high ScannedCount value with few, or no, Count results indicates an inefficient Scan operation. For more information, see Count and ScannedCount in the Amazon DynamoDB Developer Guide. If you did not use a filter in the request, then ScannedCount is the same as Count.
   */
  ScannedCount?: DynamoDB.DocumentClient.Integer;
  /**
   * The primary key of the item where the operation stopped, inclusive of the previous result set. Use this value to start a new operation, excluding this value in the new request. If LastEvaluatedKey is empty, then the "last page" of results has been processed and there is no more data to be retrieved. If LastEvaluatedKey is not empty, it does not necessarily mean that there is more data in the result set. The only way to know when you have reached the end of the result set is when LastEvaluatedKey is empty.
   */
  LastEvaluatedKey?: DynamoDB.DocumentClient.Key;
}
