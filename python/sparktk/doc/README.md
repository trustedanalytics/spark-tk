sparktk Python API doc generation
---------------------------------

sparktk uses the [pdoc](https://github.com/BurntSushi/pdoc) tool to generate API documentation.


There are 2 processing steps required in addition to calling pdoc.

1. Python pre-processing: Before pdoc is invoked, the sparktk module is copied to a tmp
   folder.  Python code in the `docutils.py` module parses and processes the tags that can
   be embedded in the doc strings, to hide or replace sections of text from the examples.  
   pdoc is pointed to this processed package.
   
2. HTML post-processing: currently pdoc does not supply multi-level navigation links in
   its HTML output (only when it runs as an http server directly).  There are two rough 'hacks'
   to get around this:
   
       (1) The templates/css.mako file, there is an "Up" link defined which points to "index.html"
   
       (2) For index.html files, post-processing must happen to change the "index.html" link
       to "..".  Again using `docutils.py`, the HTML is post-processed to make this change.  It
       is hopeful that pdoc will provide better support directly.  Also, more work could be
       done in the css and html templates to avoid this post-processing.
       
       
       
The `builddoc.sh` script automates all this work and produces the appropriate HTML documentation,
putting it in the doc/html folder

    $ builddoc.sh
