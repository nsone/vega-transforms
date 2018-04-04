import {fieldNames} from './util/util';
import {ingest, Transform} from 'vega-dataflow';
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
    	leafnode = _.leafnode;

	// remove any previous results
	out.rem = this.value;
	
	
	var flattenedData = elasticFlatten(pulse.source, leafnode);
	var fltDataLen = flattenedData.length;
	
	for(var i = 0; i < fltDataLen; i++) {
		ingest(flattenedData[i]);
	}		
	
	this.value = out.source = out.add = flattenedData
	
	// Not entirely sure we need this.  We seem to get the same results by just returning out without further operations.
	// It might be needed depending on what get's done after this transform is used, but that's unknown at the moment. :D
	var alteredFields = fieldNames([leafnode], (out.add.length ? Object.keys(out.add[0]) : []));
	return out.modifies(alteredFields);
};

function elasticFlatten(obj, leafNodeProperty, keyName) {
	var incomingArrayOfHashes = [];
	var myArrayHead = {};

	var i = 0;

	if (obj instanceof Array) {
		var arrLen = obj.length;

		for(i = 0; i < arrLen;i++) {
			var flattened = elasticFlatten(obj[i], leafNodeProperty, keyName);
			 // Whether this is the best or not for speed seems quite browser-specific, with chrome favoring concat: https://jsperf.com/multi-array-concat/7
			 // Oddly, another for-loop here seems to be the most predictable across browsers.
			incomingArrayOfHashes = incomingArrayOfHashes.concat(flattened);
		}
	} else if(obj instanceof Object) {
		var hashKey;

		/*
		 * We're going to convert aggs into properties where the property name is the agg name and the value is the value of the "key" property of the object.
		 * This is nice because then we don't have to care about the "key" property clobbring itself while recursion rolls back AND we implicitly control the property names
		 * just by changing the elastic query.
		 *
		 * For example, you have an agg named "the_date" and it contains buckets that have "key" properties unix timestamp as values.
		 * Those buckets will then be given a property called "the_date" that will contain the unix timestamp found in their "key" properties.
		*/

		if("key" in obj && keyName) {
			obj[keyName] = obj.key;
		}

		for (hashKey in obj) {
			// Short-circuiting another depth of the recursion if we know that the next thing is going to be the node we want.
			if(hashKey == leafNodeProperty) {
				incomingArrayOfHashes.push(obj[leafNodeProperty]);
			} else if(obj[hashKey] instanceof Array) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, keyName); // If it's an array, then they'll inherit the key name of the parent hash.
			} else if(obj[hashKey] instanceof Object) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, hashKey); // If it's a hash, then pass along its "name" to the next level of recursion.
			} else {
				myArrayHead[hashKey] = obj[hashKey];
			}
		}
	}

	i = 0;
	var incomingLen = incomingArrayOfHashes.length;

	// As the recursion rolls back, we need to add in the properties of the current depth to the incoming hashes.
	if(incomingLen > 0) {
		var myHeadKey;
		var currentHash;
		for(i = 0; i < incomingLen; i++) {
			currentHash = incomingArrayOfHashes[i];
			for (myHeadKey in myArrayHead) {
			  currentHash[myHeadKey] = myArrayHead[myHeadKey];  // Yes, duplicate key names means clobbering, but we shouldn't have to care.  If we start caring, we can start prepending prefixes, but that would be terrible...
			}
		}
	}

	return incomingArrayOfHashes;
}
