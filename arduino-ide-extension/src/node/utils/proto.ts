import type { Message } from 'google-protobuf';
import get = require('get-value');

export function deserializeBinaryFromGlobal(
  typeName: string
): ((data: Uint8Array) => Message) | undefined {
  return get(global, ['proto', typeName, 'deserializeBinary'].join('.'));
}
