import assert from 'node:assert/strict';
import { test } from 'node:test';
import { idChWidth } from '../src/lib/idch.ts';

test('the id column fits the widest id on the page, minimum 2', () => {
	assert.equal(idChWidth([9]), 2); // one digit: the minimum applies
	assert.equal(idChWidth([10]), 2);
	assert.equal(idChWidth([99999]), 5);
	assert.equal(idChWidth([800000]), 6);
	assert.equal(idChWidth([9, 10, 99999, 800000]), 6); // the widest wins
	assert.equal(idChWidth([]), 2);
});
