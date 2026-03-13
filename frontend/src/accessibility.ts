type KeyEventLike = {
  key: string;
  shiftKey: boolean;
  preventDefault: () => void;
};

const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(", ");

export function getFocusableElements(container: ParentNode | null): HTMLElement[] {
  if (!container) {
    return [];
  }

  return Array.from(container.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)).filter(
    (element) => {
      const style = window.getComputedStyle(element);
      return (
        !element.hasAttribute("hidden")
        && element.getAttribute("aria-hidden") !== "true"
        && style.display !== "none"
        && style.visibility !== "hidden"
        && element.getClientRects().length > 0
      );
    },
  );
}

export function trapFocus(event: KeyEventLike, container: HTMLElement | null) {
  if (event.key !== "Tab") {
    return;
  }

  const focusableElements = getFocusableElements(container);
  if (focusableElements.length === 0) {
    return;
  }

  const firstElement = focusableElements[0];
  const lastElement = focusableElements[focusableElements.length - 1];
  const activeElement = document.activeElement;

  if (!event.shiftKey && activeElement === lastElement) {
    event.preventDefault();
    firstElement.focus();
    return;
  }

  if (event.shiftKey && activeElement === firstElement) {
    event.preventDefault();
    lastElement.focus();
  }
}

export function focusByIndex(
  elements: Array<HTMLElement | null>,
  targetIndex: number,
) {
  const availableElements = elements.filter(
    (element): element is HTMLElement => element !== null,
  );
  if (availableElements.length === 0) {
    return;
  }

  const normalizedIndex =
    ((targetIndex % availableElements.length) + availableElements.length)
    % availableElements.length;
  availableElements[normalizedIndex].focus();
}

export function focusByOffset(
  elements: Array<HTMLElement | null>,
  currentElement: HTMLElement | null,
  offset: number,
) {
  const availableElements = elements.filter(
    (element): element is HTMLElement => element !== null,
  );
  if (availableElements.length === 0) {
    return;
  }

  const currentIndex = currentElement
    ? availableElements.indexOf(currentElement)
    : -1;
  const baseIndex = currentIndex === -1 ? 0 : currentIndex;
  focusByIndex(availableElements, baseIndex + offset);
}
