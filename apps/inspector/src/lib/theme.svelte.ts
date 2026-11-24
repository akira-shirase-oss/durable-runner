// The theme in effect, reactive: the stored choice (see theme.ts for the rules) and the device's scheme. The
// inline script in app.html applied the stored choice before first paint; this keeps <html data-theme> and
// localStorage in step afterwards.
import { effective, parseChoice, THEME_KEY, toggled, type Choice, type Scheme } from './theme.ts';

class ThemeState {
	choice = $state<Choice>('system');
	systemDark = $state(false);

	get scheme(): Scheme {
		return effective(this.choice, this.systemDark);
	}

	init(): () => void {
		const mq = matchMedia('(prefers-color-scheme: dark)');
		this.systemDark = mq.matches;
		const onChange = (e: MediaQueryListEvent) => (this.systemDark = e.matches);
		mq.addEventListener('change', onChange);
		let stored: string | null = null;
		try {
			stored = localStorage.getItem(THEME_KEY);
		} catch {
			stored = document.documentElement.dataset.theme ?? null;
		}
		this.choice = parseChoice(stored);
		return () => mq.removeEventListener('change', onChange);
	}

	set(choice: Choice) {
		this.choice = choice;
		const root = document.documentElement;
		if (choice === 'system') delete root.dataset.theme;
		else root.dataset.theme = choice;
		try {
			if (choice === 'system') localStorage.removeItem(THEME_KEY);
			else localStorage.setItem(THEME_KEY, choice);
		} catch {
			/* storage blocked: the page still switches for this visit */
		}
	}

	toggle() {
		this.set(toggled(this.choice, this.systemDark));
	}
}

export const theme = new ThemeState();
