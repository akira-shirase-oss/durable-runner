/** The phone layout's id-column width in ch: the digit count of the
 *  widest id on the page, minimum 2. Sizing from the rendered rows keeps
 *  the key column aligned while ids never clip (capture.mjs asserts no
 *  td.id truncates at 390px). */
export function idChWidth(ids: Iterable<string | number>): number {
	let w = 2;
	for (const id of ids) {
		const d = String(id).length;
		if (d > w) w = d;
	}
	return w;
}
