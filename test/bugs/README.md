# Bug regression tests

Every fixed bug gets one test file here, named `<area>_<symptom>_test.go`.
Each test should:

- describe the user-visible symptom and the root cause in a comment,
- fail against the code before the fix (when practical),
- use only the public `oshodi` API so it exercises the shipped surface.

Regressions that need unexported state (`internal/storage`, index rebuild
offsets, compaction artifacts) live beside the code in the relevant package.
