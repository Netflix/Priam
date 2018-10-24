# Contributing to Priam

First off, thanks for taking the time to contribute! 

The following is a set of guidelines for contributing to Priam on GitHub. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

#### Table Of Contents

[Questions](#questions)

[How Can I Contribute?](#how-can-i-contribute)
  * [Reporting Bugs](#reporting-bugs)
  * [Suggesting Enhancements](#suggesting-enhancements)
  * [Your First Code Contribution](#your-first-code-contribution)
  * [Pull Requests](#pull-requests)

## Questions

Please feel free to file an issue on github repo, if one is not already covered in this documentation. 

## How Can I Contribute?
### Reporting Bugs

This section guides you through submitting a bug report for Priam. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

Before creating bug reports, please check [this list](#before-submitting-a-bug-report) as you might find out that you don't need to create one. When you are creating a bug report, please [include as many details as possible](#how-do-i-submit-a-good-bug-report). Fill out [the required template](ISSUE_TEMPLATE.md), the information it asks for helps us resolve issues faster.

> **Note:** If you find a **Closed** issue that seems like it is the same thing that you're experiencing, open a new issue and include a link to the original issue in the body of your new one.

#### Before Submitting A Bug Report

* Check if you can reproduce the problem [in the latest version of Priam. 
* **Check the [FAQs on the forum](../faq/faq.md)** for a list of common questions and problems.
* Perform a search to see if the problem has already been reported. If it has **and the issue is still open**, add a comment to the existing issue instead of opening a new one.

#### How Do I Submit A (Good) Bug Report?

Bugs are tracked as [GitHub issues](https://guides.github.com/features/issues/). Create an issue and provide the following information by filling in [the template](../template/bug_report.md).

Explain the problem and include additional details to help maintainers reproduce the problem:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible. 
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples. If you're providing snippets in the issue, use [Markdown code blocks](https://help.github.com/articles/markdown-basics/#multiple-lines).
* **Explain which behavior you expected to see instead and why.**
* **Version of the Priam and Cassandra used** 
* **Did the problem start happening recently** (e.g. after updating to a new version of Priam or Cassandra) or was this always a problem?

### Suggesting Enhancements

Enhancement suggestions are tracked as [GitHub issues](https://guides.github.com/features/issues/) and filling out the [template](../template/feature_request.md)

### Your First Code Contribution

Unsure where to begin contributing to Priam? You can start by looking through these `beginner` and `help-wanted` issues. 

### Pull Requests

Please follow these steps to have your contribution considered by the maintainers: 
1. Priam uses google java styling to maintain the project. Ensure that your code adheres to that. 
2. Ensure that your code has all the comments at appropriate places describing the (chagne in) behavior. 
3. Ensure that your code has good java documentation. 
4. After you submit your pull requests, verify that all status checks are passing on Travis CI. 