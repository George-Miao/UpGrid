export type ValidatableControl = HTMLElement & {
  readonly validationMessage: string;
  readonly validity: ValidityState;
};

function controlLabel(control: ValidatableControl): string | undefined {
  const labels = "labels" in control ? (control as HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement).labels : null;
  return control.getAttribute("aria-label") ?? labels?.item(0)?.textContent?.trim() ?? undefined;
}

export function controlValidationMessage(control: ValidatableControl): string {
  const label = controlLabel(control);
  if (control.validity.valueMissing && label) return `Please fill out ${label.toLocaleLowerCase()}`;
  return label ? `${label}: ${control.validationMessage}` : control.validationMessage;
}

export function updatePasswordConfirmationValidity(form: HTMLFormElement): void {
  const password = form.elements.namedItem("password");
  const confirmation = form.elements.namedItem("password_confirmation");
  if (!(password instanceof HTMLInputElement) || !(confirmation instanceof HTMLInputElement)) return;
  confirmation.setCustomValidity(password.value === confirmation.value ? "" : "Passwords do not match.");
}
