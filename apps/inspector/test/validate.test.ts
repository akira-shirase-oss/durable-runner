import assert from 'node:assert/strict';
import { test } from 'node:test';
import { b64ToText, confirmError, deletePhrase, emailError, handlerUrlError, inputToB64, nameError, passwordError } from '../src/lib/validate.ts';

test('email: one @ with text on both sides, at most 254', () => {
	assert.equal(emailError('ada@example.com'), null);
	assert.equal(emailError('  ada@example.com '), null);
	assert.match(emailError('') ?? '', /Enter your email/);
	for (const bad of ['ada', '@example.com', 'ada@', 'a@b@c', 'a da@example.com']) assert.ok(emailError(bad), bad);
	assert.ok(emailError(`${'a'.repeat(250)}@b.io`));
});

test('password: 12–128 characters as the server counts them, no composition rules', () => {
	assert.equal(passwordError('correct horse'), null);
	assert.match(passwordError('short') ?? '', /at least 12 characters \(5 so far\)/);
	assert.equal(passwordError('ü'.repeat(12)), null, 'twelve characters, more bytes');
	assert.equal(passwordError('😀'.repeat(12)), null, 'astral characters count once');
	assert.ok(passwordError('a'.repeat(129)));
	assert.equal(passwordError('a'.repeat(128)), null);
	assert.ok(passwordError(''));
	assert.equal(confirmError('abc', 'abc'), null);
	assert.ok(confirmError('abc', 'abd'));
	assert.ok(confirmError('abc', ''));
});

test('names: 1–128 characters, no control characters', () => {
	assert.equal(nameError('Production'), null);
	assert.ok(nameError('   '));
	assert.ok(nameError('a'.repeat(129)));
	assert.ok(nameError('tab\there'));
	assert.match(nameError('', 'handler name') ?? '', /Enter a handler name/);
});

test('handler URLs: absolute http(s) or the sample handler', () => {
	assert.equal(handlerUrlError('https://example.com/step'), null);
	assert.equal(handlerUrlError('http://example.com:8080/x'), null);
	assert.equal(handlerUrlError('builtin:echo'), null);
	for (const bad of ['', 'example.com/step', 'ftp://example.com', 'https://user:pw@example.com/', 'builtin:other', 'javascript:alert(1)']) assert.ok(handlerUrlError(bad), bad);
});

test('delete phrase', () => {
	assert.equal(deletePhrase('Production'), 'delete Production');
});

test('inputs: JSON text to base64 and back', () => {
	assert.deepEqual(inputToB64(''), { b64: '', error: null });
	assert.deepEqual(inputToB64('{"a":1}'), { b64: 'eyJhIjoxfQ==', error: null });
	assert.equal(inputToB64('{"é":"ü"}').b64, Buffer.from('{"é":"ü"}').toString('base64'));
	assert.ok(inputToB64('{nope').error);
	assert.ok(inputToB64(JSON.stringify('x'.repeat(70_000))).error);
	assert.equal(b64ToText('eyJhIjoxfQ=='), '{"a":1}');
	assert.equal(b64ToText(Buffer.from([0xff, 0xfe]).toString('base64')), null);
	assert.equal(b64ToText(null), null);
});
