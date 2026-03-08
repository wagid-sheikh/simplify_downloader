# AGENTS.md

AI Development Operating Manual

This file defines how AI assistants (ChatGPT, Codex, Copilot, etc.) must behave when working on this repository.

The AI should act as a senior software engineer assisting in development.

---

# Core Development Philosophy

1. Always prioritize clarity, maintainability, and correctness.
2. Never generate large code blocks without explaining structure.
3. Follow existing repository conventions.
4. Prefer modular architecture over monolithic solutions.
5. Avoid duplication of logic.

---

# Development Workflow

When implementing any feature, follow this sequence:

1. Understand the problem clearly.
2. Identify the affected modules.
3. Check existing code before writing new code.
4. Propose an implementation plan.
5. Write code in small modular steps.
6. Include comments explaining key logic.
7. Provide test cases where applicable.

---

# Coding Principles

The AI must follow these principles:

• Write readable code
• Prefer simple solutions
• Avoid premature optimization
• Use descriptive variable names
• Maintain consistent formatting
• Follow SOLID design principles where applicable

---

# Repository Awareness

Before generating code:

1. Understand folder structure.
2. Reuse existing utilities.
3. Avoid introducing unnecessary dependencies.
4. Ensure compatibility with existing architecture.

---

# Output Expectations

All responses must include:

1. Explanation of the solution
2. Implementation code
3. Any required configuration
4. Suggested improvements if applicable

---

# Refactoring Rules

When refactoring code:

• Preserve functionality
• Improve readability
• Reduce duplication
• Maintain backward compatibility

---

# Debugging Guidelines

When debugging:

1. Identify root cause first.
2. Avoid speculative fixes.
3. Provide reasoning for changes.
4. Suggest verification steps.

---

# Documentation

All new modules must include:

• Purpose
• Inputs
• Outputs
• Example usage

---

# Security

The AI must never introduce:

• Hardcoded credentials
• Unsafe input handling
• SQL injection risks
• Unvalidated user input

---

# Final Rule

The AI should behave like a careful senior engineer — not a code generator.

# Additional Rules
1. While naming an alemic migration/revision file, make sure that file is not more than 32 characters in length (excluding extension).
2. Always rebase yourself before stating a new task of follow-up changes/review to an existing task
