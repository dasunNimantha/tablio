import type { TestingLibraryMatchers } from "@testing-library/jest-dom/matchers";

// jest-dom's bundled vitest types augment `Assertion<T>`, which no longer
// matches vitest 5's `Assertion<R, T>`; custom matchers now hang off `Matchers`.
declare module "vitest" {
  interface Matchers<R extends void | Promise<void> = void | Promise<void>, T = unknown>
    extends TestingLibraryMatchers<any, R> {}
}
