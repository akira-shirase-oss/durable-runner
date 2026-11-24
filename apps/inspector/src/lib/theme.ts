// The theme choice, pure, with the landing site's behaviour: no stored choice means the device's setting decides;
// pressing the toggle stores the other scheme, and pressing it onto the device's own scheme clears the choice so
// the page follows the device again. The inline script in app.html applies a stored choice before first paint
// (and performs migrateStored there too, in a few lines of its own).

export type Scheme = 'light' | 'dark';
export type Choice = 'system' | Scheme;

/** localStorage key, the landing's. */
export const THEME_KEY = 'theme';
/** The inspector's old key (it stored every toggle; unset meant dark). */
export const OLD_THEME_KEY = 'durable-theme';

export function effective(choice: Choice, systemDark: boolean): Scheme {
	return choice === 'system' ? (systemDark ? 'dark' : 'light') : choice;
}

/** The toggle: flip the scheme in effect; landing on the device's own scheme means "follow the device". */
export function toggled(choice: Choice, systemDark: boolean): Choice {
	const next: Scheme = effective(choice, systemDark) === 'dark' ? 'light' : 'dark';
	return next === (systemDark ? 'dark' : 'light') ? 'system' : next;
}

/** A stored value as a choice: only "light" or "dark" count. */
export function parseChoice(v: string | null | undefined): Choice {
	return v === 'light' || v === 'dark' ? v : 'system';
}

/** One-time migration of the old key: an old explicit choice that differs from the device's scheme is kept under
 *  the new key (unless a new choice already exists); one equal to it becomes "follow the device". The old key is
 *  always removed. Returns what to store under THEME_KEY (null: nothing). */
export function migrateStored(old: string | null, current: string | null, systemDark: boolean): Scheme | null {
	if (current === 'light' || current === 'dark') return current;
	if (old !== 'light' && old !== 'dark') return null;
	return old === (systemDark ? 'dark' : 'light') ? null : old;
}
