import {fieldNames} from './util/util';
import {derive, Transform} from 'vega-dataflow';
import {inherits} from 'vega-util';

/**
 * Flattens typical multi-agg ElasticSearch query results.
 * @constructor
 * @param {object} params - The parameters for this operator.
 * @param {string} [params.leafnode] - The hash/dict at the deepest
 *   aggregation level that holds the values that you want to graph.
 */
 
export default function ElasticFlatten(params) {
  Transform.call(this, [], params);
}

ElasticFlatten.Definition = {
  "type": "ElasticFlatten",
  "metadata": {"generates": true, "source": true},
  "params": [
    { "name": "leafnode", "type": "string", "array": false, "required": true }
  ]
};

var prototype = inherits(ElasticFlatten, Transform);

prototype.transform = function(_, pulse) {
  var out = pulse.fork(pulse.NO_SOURCE),
      leafnode = fieldNames(fields, _.leafnode);

  // remove any previous results
  out.rem = this.value;

  this.value = out.source = elasticFlatten(this.value, leafnode);
  return out;
};

function elasticFlatten(obj, leafNodeProperty, keyName="none") {
	var incomingArrayOfHashes = [];
	var myArrayHead = {};

   	var i = 0;
   	   
	if (obj instanceof Array) {
		var arrLen = obj.length;
		
		for(i = 0; i < arrLen;i++) {
			incomingArrayOfHashes.push.apply(incomingArrayOfHashes, elasticFlatten.call(this,obj[i], leafNodeProperty, keyName));
		}
	} else if(obj instanceof Object) {
		var hashKey;
		
		if("key" in obj) {
			obj[keyName] = obj.key;
		}
		
   		for (hashKey in obj) {
			if(hashKey == leafNodeProperty) {
				incomingArrayOfHashes.push(obj[leafNodeProperty]);
			} else if(obj[hashKey] instanceof Array) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, keyName);
			} else if(obj[hashKey] instanceof Object) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, hashKey);
			} else {   		
				myArrayHead[hashKey] = obj[hashKey];
			}
		}
	}
	
	i = 0;
	var incomingLen = incomingArrayOfHashes.length;
	  
	
	if(incomingLen > 0) {
		var myHeadKey;
		var currentHash;
		for(i = 0; i < incomingLen; i++) {
			currentHash = incomingArrayOfHashes[i];
			for (myHeadKey in myArrayHead) {
			  currentHash[myHeadKey] = myArrayHead[myHeadKey];
			}
		}
	}

	return incomingArrayOfHashes;
}