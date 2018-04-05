import {fieldNames} from './util/util';
import {ingest, Transform} from 'vega-dataflow';
import {inherits} from 'vega-util';

/**
 * Flattens typical multi-agg ElasticSearch query results.
 * @constructor
 * @param {object} params - The parameters for this operator.
 * @param {string} [params.leafnode] - The hash/dict at the deepest
 *   aggregation level that holds the values that you want to graph.
 * @param {boolean} [params.noclobber] - Boolean that describes whether agg name should be prepended to keys to avoid clobbering values as flattening happens 
 * @param {string} [params.ignorefields] - An array of strings of fields that should be ignored.
 */

export default function ElasticFlatten(params) {
  Transform.call(this, [], params);
}

ElasticFlatten.Definition = {
  "type": "ElasticFlatten",
  "metadata": {"generates": true, "source": true},
  "params": [
		{ "name": "leafnode", "type": "string", "array": false, "required": true },
		{ "name": "noclobber", "type": "boolean", "array": false, "required": false, "default": false },
		{ "name": "ignorefields", "type": "string", "array": true, "required": false, "default": [] } // ignorefields instead of includefields because flattened fields would be thrown out by default otherwise.
  ]
};

var prototype = inherits(ElasticFlatten, Transform);

prototype.transform = function(_, pulse) {
	var out = pulse.fork(pulse.NO_SOURCE),
    	leafnode = _.leafnode,
    	noclobber = _.noclobber || false,
    	ignorefields = _.ignorefields || [];

	// remove any previous results
	out.rem = this.value;
	
	var ignores = {};
	
	for(var i = 0; i < ignorefields.length; i++) {
		ignores[ignorefields[i]] = true;
	}

	this.value = out.source = out.add = elasticFlatten(pulse.source, leafnode, "", noclobber, ignores);
	
	// Not entirely sure we need this.  We seem to get the same results by just returning out without further operations.
	// It might be needed depending on what get's done after this transform is used, but that's unknown at the moment. :D
	var alteredFields = fieldNames([leafnode], (out.add.length ? Object.keys(out.add[0]) : []));
	return out.modifies(alteredFields);
};

function elasticFlatten(obj, leafNodeProperty, keyName, noclobber, ignores) {
	var incomingArrayOfHashes = [];
	var myArrayHead = {};

	var i = 0;

	if (obj instanceof Array) {
		var arrLen = obj.length;

		for(i = 0; i < arrLen;i++) {
			var flattened = elasticFlatten(obj[i], leafNodeProperty, keyName, noclobber, ignores);
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
				incomingArrayOfHashes.push(ingest(obj[leafNodeProperty])); // ingest() is a vega thing.  Doing it here so that we don't have to re-loop through everything later to ingest the entries.
			} else if(obj[hashKey] instanceof Array) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, keyName, noclobber, ignores); // If it's an array, then they'll inherit the key name of the parent hash.
			} else if(obj[hashKey] instanceof Object) {
				incomingArrayOfHashes = elasticFlatten(obj[hashKey], leafNodeProperty, hashKey, noclobber, ignores); // If it's a hash, then pass along its "name" to the next level of recursion.
			} else {
				// We only want to ignore things that aren't potential containers of leafNodeProperty to prevent people from shooting themselves in the foot, even though they could find other ways.
				// We also only ignore things that are not present IN target leaf nodes, whether this is 100% true depends on whether noclobber is used since fields could still be tossed out as recursion rolls back.
				if(!ignores[hashKey]) {
					myArrayHead[hashKey] = obj[hashKey];
				}
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
				if(noclobber && keyName != myHeadKey && (myHeadKey in currentHash)) {
					currentHash[keyName + "_" + myHeadKey] = myArrayHead[myHeadKey];
				} else {
					currentHash[myHeadKey] = myArrayHead[myHeadKey];
				}
			}
		}
	}

	return incomingArrayOfHashes;
}
