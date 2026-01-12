# Allspring Global - GitHub Enterprise Cloud EMU Migration

## Executive Overview

Thanks everyone for joining. I'll be leading the GitHub Enterprise migration for Allspring Global, specifically the transition into **GitHub Enterprise Cloud** using **Enterprise Managed Users (EMU)**.

### Session Goals
- Walk through how the migration works
- Be very clear about what data migrates automatically and what does not
- Explain the phased approach we follow to minimize risk
- Align on expectations, validation points, and responsibilities

---

## Understanding EMU Migration

**This is not just a repository copy.**

Because this is an EMU-based migration, GitHub separates repository data from identity and access.

### Prerequisites
- ✅ Target EMU tenant exists
- ✅ Enterprise created
- ✅ EMU enabled
- ✅ Target organization(s) created
- ✅ Identity Provider (IdP) integration is configured

---

## What Gets Migrated

### ✅ Automatically Migrated via GitHub Enterprise Importer

The following items migrate **automatically** with full fidelity:

- **Repository content and full Git history**
- **Commits, branches, and tags**
- **Pull requests** with full timeline and reviews
- **Issues** with labels, milestones, comments, and state
- **Commit comments and PR comments**
- **Releases and release assets** (within size limits)
- **Repository topics**
- **Repository archival status** (preserved)

---

### ⚠️ Migrates But Requires Validation

> **Note:** These items technically migrate, but need post-migration validation to function correctly in EMU.

#### CODEOWNERS
- ✅ File migrates
- ⚠️ Owners must be mapped to EMU users or teams

#### Git Submodules
- ✅ `.gitmodules` file migrates
- ⚠️ URLs must be validated and sometimes updated

#### Classic Branch Protections
- ⚠️ May partially migrate
- 🔍 Always validated and standardized post-migration

> **Important:** These are not blockers, but they are part of our validation checklist.

---

### ❌ Not Migrated (Manual/Scripted Setup Required)

The following items require **manual configuration** or **scripted setup** in the target EMU environment:

- **Users and user profiles**
- **User permissions** at org/repo/branch level
- **Teams** (groups) and team permissions
- **Rulesets** (branch and repo governance)
- **Secrets, variables, and environments**
- **Webhooks and deploy keys**
- **GitHub Actions runners**
- **Git LFS objects** (require separate transfer)

---

## Migration Approach

Our proven phased migration process minimizes risk and ensures success.

### Phase 1: Discovery & Inventory

> "We start with a full inventory of repositories and metadata."

**Inventory Includes:**
- Repo count and size
- Active vs archived repos
- LFS usage
- Submodules
- Branch protection complexity
- PR and issue volume

---

### Phase 2: Analysis & Risk Identification

> "Next, we analyze the inventory to identify anything that needs special handling."

**Risk Analysis Covers:**
- Git LFS repos
- Submodules with private dependencies
- Large repos or large releases
- Complex branch protection or rulesets
- Repos requiring sequencing

---

### Phase 3: Migration Waves

> "We never migrate everything at once."

**Wave Strategy:**
- ✅ Repos grouped into logical waves
- ✅ Low-risk repos first
- ✅ High-risk or business-critical repos later
- ✅ Allows feedback and tuning between waves

---

### Phase 4: Dry Runs (Dry-Run Org)

> "Before touching production, we perform dry runs into a non-production org."

**Dry Run Purpose:**
- Validate data fidelity
- Test LFS and submodules
- Validate ruleset recreation
- Confirm EMU identity mapping
- Confirm developer experience

---

### Phase 5: Production Migration

> "Once dry runs are successful, we repeat the same process into the production EMU org."

**Production Execution:**
- ✅ Same tooling
- ✅ Same scripts
- ✅ Same validation
- ✅ Minimal surprises

---

## Validation & Success Criteria

> "Migration success is measured by validation, not just completion."

### Success Validation Checklist

| Validation Item | Status |
|----------------|--------|
| Repo counts match | ✅ |
| PRs and issues visible | ✅ |
| Branches and tags present | ✅ |
| Archived repos remain archived | ✅ |
| Rulesets applied as designed | ✅ |
| Developers can clone, build, and contribute | ✅ |

---

## Summary

This migration follows a **proven, risk-minimized approach** that ensures:
- ✅ Complete data fidelity
- ✅ Controlled, phased execution
- ✅ Thorough validation at every step
- ✅ Minimal disruption to development teams
- ✅ Clear success criteria

**Questions?**
