import { describe, it } from 'mocha';
import { expect } from 'chai';
import { compare } from './compare';

describe('compare function', () => {
    it('returns -1 when first value is less than second', () => {
        expect(compare(1, 2)).to.equal(-1);
        expect(compare('a', 'b')).to.equal(-1);
        expect(compare(null, 1)).to.equal(-1);
    });

    it('returns 1 when first value is greater than second', () => {
        expect(compare(2, 1)).to.equal(1);
        expect(compare('b', 'a')).to.equal(1);
        expect(compare(1, null)).to.equal(1);
    });

    it('returns 0 when values are equal', () => {
        expect(compare(1, 1)).to.equal(0);
        expect(compare('a', 'a')).to.equal(0);
        expect(compare(null, null)).to.equal(0);
        expect(compare(undefined, undefined)).to.equal(0);
    });
}); 