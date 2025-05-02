# Contributing

The best documentation is the result of collaboration between users and developers with different perspectives.
As developers, it is easy for us to assume users have certain background knowledge that, in reality, we only
have because we are so immersed in this codebase.
In general, if you see something that is unclear, missing, or out of date in this book, please
[open an issue](https://github.com/TCCON/ggg-rs/issues) or, even better, a [pull request](https://github.com/TCCON/ggg-rs/pulls)
on the GGG-RS repo, with the "documentation" tag.

## Contribution criteria

Requests to update the documentation will be evaluated on several criteria, primarily:

- accuracy (does the change correctly reflect the behavior of the program?),
- clarity (is the change easy to understand and as concise as possible?), and
- scope (does the change add information that should be included in that location, or is it better suited to another source?)

When opening an issue, providing as much information as possible will help it be resolved quickly,
as will responding promptly when asked for more input. Likewise, when opening a pull request, providing
a clear description of what was changed and why will help us review it efficiently. If you are providing
a pull request, please verify that the edits render correctly by following the instructions in [Building the book](#building-the-book),
below.

We reserve the right to turn down requests for changes if, in our opinion, they make the documentation worse,
or if the requestor does not provide sufficient information to make the needed change clear.
Well explained and respectful requests will usually be accepted.

## Common types of questions

Below are some common questions and details on what sort of information to provide when asking for an update to
help us resolve the problem efficiently.

### The information I need is not where I expected

When opening an issue, be clear about what information you are trying to find, where you expected to find
it in the documentation, and why you were looking there.
Understanding how you expect the information to be organized helps us examine if there might be other ways
we need to connect different parts of the documentation.
Generally, the best fix for this problem is to identify where a link between parts of the documentation will
help guide people to the correct page.
Other solutions may be appropriate in more complicated cases.

### A program is not included in the book

First, please check that it is one of the GGG-RS programs.
This means it will have a folder under the `src/bin` subdirectory of the [repository](https://github.com/TCCON/ggg-rs).
The GGG-RS programs will coexist with regular GGG and EGI-RS programs in `$GGGPATH/bin/`, so just because a program
exists there does not mean it will be documented here.

If a program really is missing, then either open an issue or create a pull request that add it.
If creating a pull request, please match the structure of the existing programs' documentation.

### I do not understand what the documentation is trying to explain

When you encounter something that is not clear, please first try to figure it out yourself by following any
examples and testing things out.
If that section of the documentation links to external resources (e.g., the [TOML format](https://toml.io/en/),
please review those resources as well.

If something truly is unclear, then open an issue and do your best to explain what you were trying to accomplish
and what you found difficult to undestand. 
Explaining the overall task you were trying to accomplish, as well as the part of the documentation that you
found unclear, will help us identify if this is an [XY problem](https://en.wikipedia.org/wiki/XY_problem), where
the reason it was unclear is because there is a better solution to your task than the one you were trying to use.

## Building the book

If you want to edit this book, it lives in the `book` subdirectory of the [GGG-RS repo](https://github.com/TCCON/ggg-rs).
The source code is in Markdown, and can be edited with any plain text editor (e.g., Notepad, TextEdit, vim, nano) or
code editor (e.g., VSCode).
It is built with [mdbook](https://github.com/rust-lang/mdBook) with the additional 
[mdbook-admonish](https://github.com/tommilligan/mdbook-admonish) preprocessor.

To check that the book renders correctly when making edits:

- Install both `mdbook` and `mdbook-admonish` follow the instructions in their repositories.
- In the `book` subdirectory of GGG-RS repository (which you will have cloned to your computer), run `mdbook serve`.
- Copy the "localhost" link it prints and paste it into any web browser.

You will see the rendered local copy of the book, and it will automatically update each time you make changes.

Please do this before submitting any pull requests, as it will slow things down significantly if we have to iterate
multiple times with you to ensure the book builds correctly.
