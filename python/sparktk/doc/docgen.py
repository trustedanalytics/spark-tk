# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Pre-processes Python and post-processes HTML for doc and doctests generation
"""
import re
import datetime
import os
import sys
import shutil
import tempfile
import logging
logger = logging.getLogger(__file__)


def parse_for_doc(text, file_name=None):
    return str(DocExamplesPreprocessor(text, mode='doc', file_name=file_name))


def parse_for_doctest(text, file_name=None):
    return str(DocExamplesPreprocessor(text, mode='doctest', file_name=file_name))


class DocExamplesException(Exception):
    """Exception specific to processing documentation examples"""
    pass


class DocExamplesPreprocessor(object):
    """
    Processes text (intended for Documentation Examples) and applies ATK doc markup, mostly to enable doctest testing
    """

    doctest_ellipsis = '-etc-'  # override for the doctest ELLIPSIS_MARKER

    # multi-line tags
    hide_start_tag = '<hide>'
    hide_stop_tag = '</hide>'
    skip_start_tag = '<skip>'
    skip_stop_tag = '</skip>'

    # replacement tags
    doc_replacements = [('<progress>', '[===Job Progress===]'),
                        ('<connect>', 'Connected ...'),
                        ('<datetime.datetime>', repr(datetime.datetime.now())),
                        ('<blankline>', '<BLANKLINE>')]   # sphinx will ignore this for us

    doctest_replacements = [('<progress>', doctest_ellipsis),
                            ('<connect>', doctest_ellipsis),
                            ('<datetime.datetime>', doctest_ellipsis),
                            ('<blankline>', '<BLANKLINE>')]

    # Two simple fsms, each with 2 states:  Keep, Drop
    keep = 0
    drop = 1

    def __init__(self, text, mode='doc', file_name=None):
        """
        :param text: str of text to process
        :param mode:  preprocess mode, like 'doc' or 'doctest'
        :return: object whose __str__ is the processed example text
        """
        if mode == 'doc':
            # process for human-consumable documentation
            self.replacements = self.doc_replacements
            self.is_state_keep = self._is_hide_state_keep
            self._disappear  = ''   # in documentation, we need complete disappearance
        elif mode == 'doctest':
            # process for doctest execution
            self.replacements = self.doctest_replacements
            self.is_state_keep = self._is_skip_state_keep
            self._disappear = '\n'  # disappear means blank line for doctests, to preserve line numbers for error report
        else:
            raise DocExamplesException('Invalid mode "%s" given to %s.  Must be in %s' %
                                       (mode, self.__class__, ", ".join(['doc', 'doctest'])))
        self.skip_state = self.keep
        self.hide_state = self.keep
        self.processed = ''
        self._file_name = file_name

        if text:
            lines = text.splitlines(True)
            self.processed = ''.join(self._process_line(line) for line in lines)
            if self.hide_state != self.keep:
                raise DocExamplesException("unclosed tag %s found%s" % (self.hide_start_tag, self._in_file()))
            if self.skip_state != self.keep:
                raise DocExamplesException("unclosed tag %s found" % self.skip_start_tag, self._in_file())

    def _in_file(self):
        return (" in file %s" % self._file_name) if self._file_name else ''

    def _is_skip_state_keep(self):
        return self.skip_state == self.keep

    def _is_hide_state_keep(self):
        return self.hide_state == self.keep

    def _process_line(self, line):
        """processes line and advances fsms as necessary, returns processed line text"""
        stripped = line.lstrip()
        if stripped:

            # Repair the "Up" link for certain files (this needs to match the doc/templates/css.mako)
            if self._file_name and self._file_name.endswith("/index.html") and '<a href="index.html" id="fixed_top_left">Up</a>' in line:
                if self._file_name.endswith("/sparktk/index.html"):
                    return '  <!-- No Up for root level index.html -->\n'
                return '<a href="../index.html" id="fixed_top_left">Up</a>\n'

            stripped = DocExamplesPreprocessor._strip_markdown_comment(stripped)
            if stripped[0] == '<':
                if self._process_if_tag_pair_tag(stripped):
                    return self._disappear  # tag-pair markup should disappear appropriately

                # check for keyword replacement
                for keyword, replacement in self.replacements:
                    if stripped.startswith(keyword):
                        line = line.replace(keyword, replacement, 1)
                        break

        return line if self.is_state_keep() else self._disappear

    def _process_if_tag_pair_tag(self, stripped):
        """determines if the stripped line is a tag pair start or stop, advances fsms accordingly"""
        if stripped.startswith(self.skip_start_tag):
            if self.skip_state == self.drop:
                raise DocExamplesException("nested tag %s found%s" % (self.skip_start_tag, self._in_file()))
            self.skip_state = self.drop
            return True
        elif stripped.startswith(self.skip_stop_tag):
            if self.skip_state == self.keep:
                raise DocExamplesException("unexpected tag %s found%s" % (self.skip_stop_tag, self._in_file()))
            self.skip_state = self.keep
            return True
        elif stripped.startswith(self.hide_start_tag):
            if self.hide_state == self.drop:
                raise DocExamplesException("nested tag %s found%s" % (self.hide_start_tag, self._in_file()))
            self.hide_state = self.drop
            return True
        elif stripped.startswith(self.hide_stop_tag):
            if self.hide_state == self.keep:
                raise DocExamplesException("unexpected tag %s found%s" % (self.hide_stop_tag, self._in_file()))
            self.hide_state = self.keep
            return True
        return False

    markdown_comment_tell = r'[//]:'
    markdown_comment_re = r'^\[//\]:\s*#\s*\"(.+)\"$'
    markdown_comment_pattern = re.compile(markdown_comment_re)

    @staticmethod
    def _strip_markdown_comment(s):
        """
        Checks if the given string is formatted as a Markdown comment per Magnus' response here:
        http://stackoverflow.com/questions/4823468/comments-in-markdown/32190021#32190021

        If it is, the formatting is stripped and only the comment's content is returned
        If not, the string is returned untouched
        """
        if s.startswith(DocExamplesPreprocessor.markdown_comment_tell):
            m = DocExamplesPreprocessor.markdown_comment_pattern.match(s)
            if m:
                return m.group(1)
        return s

    def __str__(self):
        return self.processed


##############################################################
# py and html processing:
##############################################################

def pre_process_py(path):

    def py_preprocessor(full_name, reader, writer):
        text = reader.read()
        output = str(DocExamplesPreprocessor(text, mode='doc', file_name=full_name))
        writer.write(output)
    walk_path(path, '.py', py_preprocessor)


def post_process_html(path):

    def html_predicate(full_name):
        return full_name.endswith("/index.html")

    def html_index_postprocessor(full_name, reader, writer):
        for line in reader.readlines():
            line = repair_index_up(line, full_name)
            writer.write(line)
    walk_path(path, '.html', html_index_postprocessor, html_predicate)


def walk_path(path, suffixes, processor, full_name_predicate=None):
    """walks the path_to_examples and creates paths to all the .rst files found"""
    logger.debug("walk_path(path='%s', suffixes=%s)", path, suffixes)
    for root, dir_names, file_names in os.walk(path):
        logger.debug("walk_path: file_names=%s", file_names)
        for file_name in file_names:
            if file_name.endswith(suffixes):
                full_name = os.path.join(root, file_name)
                #logger.debug("walk_path: processing file %s", full_name)
                process_file(full_name, processor, full_name_predicate)


def process_file(full_name, processor, full_name_predicate=None):
    """open file, process it, write it back"""
    if full_name_predicate is None or full_name_predicate(full_name):
        logger.debug("process_file: processing file %s", full_name)
        with open(full_name, 'r') as r:
            with tempfile.NamedTemporaryFile(delete=False) as w:
                tmp_name = w.name
                #logger.debug("process_file: tmp_name=%s", tmp_name)
                processor(full_name, r, w)
        os.remove(full_name)
        shutil.move(tmp_name, full_name)


def repair_index_up(line, full_name, for_main=False):
    # Repair the "Up" link for certain files (this needs to match the doc/templates/css.mako)
    if '<a href="index.html" id="fixed_top_left">Up</a>' in line:
        if full_name.endswith("/sparktk/index.html"):
            if for_main:
                return '  <!-- No Up for root level index.html -->\n'  # This is for the absolute main index, no 'Up'
            else:
                return '<a href="../../index.html" id="fixed_top_left">Up</a>\n'  # ../.. to go up past full/sparktk
        return '<a href="../index.html" id="fixed_top_left">Up</a>\n'

    return line


def process_line_html(line, full_name):

    if full_name.endswith("/index.html"):
        line = repair_index_up(line, full_name)

    return line



class MainApiDocs(object):
    """Copies the docs for the Main APIs out the full documentation and puts them in the forefront for the user"""

    # This is the main body of the index, hand-written.  Make mods here:
    main_index_body = """
<body>
<div id="container">
    <div id="sidebar">
        <h1>Index</h1>
        <ul id="index">
           <li class="set"><h3><a href="#header-submodules">Main APIs</a></h3>
                <ul>
                    <li class="mono"><a href="dicom.m.html">dicom</a></li>
                    <li class="mono"><a href="frame.m.html">frame</a></li>
                    <li class="mono"><a href="graph.m.html">graph</a></li>
                    <li class="mono"><a href="models/index.html">models</a></li>
                    <li class="mono"><a href="tkcontext.m.html">tkcontext</a></li>
                </ul>
            </li>
        </ul>
    </div>

    <article id="content">
        <div>
            <header id="section-intro">
                <h1 class="title"><span class="name">sparktk</span></h1>
            </header>

            <section id="section-items">
            (Note: This is documentation for the main Python APIs.  For package details, see
            <a href="full/sparktk/index.html">the complete python docs</a>)
            <br>
            %s
            </section>
        </div>
        <div class="clear" />
        <footer id="footer">
            <div>
                spark-tk Python API Documentation
            </div>
        </footer>
    </article>
</div>
</body>
"""

    def __init__(self, html_dir):
        """processes the full_package docs to include the Main APIs and present them in the forefront"""
        from distutils.dir_util import copy_tree
        import shutil

        # start with full_package docs

        # move full_package_dir down a level
        logger.debug("rework_for_main('%s')", html_dir)
        self.html_dir = html_dir
        self.doc_root_parent_dir = os.path.abspath(os.path.join(self.html_dir, os.pardir))
        self.tmp_html_dir = os.path.join(self.doc_root_parent_dir, "tmp-html")
        try:
            shutil.rmtree(self.tmp_html_dir)
        except:
            pass

        logger.debug("os.rename('%s', '%s')", self.html_dir, self.tmp_html_dir)
        os.rename(self.html_dir, self.tmp_html_dir)
        logger.debug("os.mkdir('%s')" % self.html_dir)
        os.mkdir(self.html_dir)

        # cp up the files interesting for "Main APIs"

        user_files = ["sparktk/frame/frame.m.html",
                      "sparktk/graph/graph.m.html",
                      "sparktk/dicom/dicom.m.html",
                      "sparktk/tkcontext.m.html"]
        for file_path in user_files:
            src = os.path.join(self.tmp_html_dir, file_path)
            dst = self.html_dir
            logger.debug("shutil.copy('%s', '%s')", src, dst)
            shutil.copy(src, dst)

        user_dirs = [("sparktk/models", "models")]
        for dir_path, dir_name in user_dirs:
            src = os.path.join(self.tmp_html_dir, dir_path)
            dst = os.path.join(self.html_dir, dir_name)
            logger.debug("copy_tree('%s', '%s')", src, dst)
            copy_tree(src, dst)

        self._post_process_for_main(self.html_dir)
        self._make_main_index_html()
        self._patch_main_tkcontext_html()
        self._patch_full_index_html()

        # move the tmp-html dir back under the new html dir, as the full package docs
        self.full_package_dir = os.path.join(self.html_dir, "full")
        logger.debug("os.rename('%s', '%s')", self.tmp_html_dir, self.full_package_dir)
        os.rename(self.tmp_html_dir, self.full_package_dir)

    def _make_main_index_html(self):
        """
        Creates the main index.html for the landing page
        """

        # copy the main index from the original html
        src = os.path.join(self.tmp_html_dir, "sparktk/index.html")
        dst = os.path.join(self.html_dir, "index.html")
        try:
            readme_src = os.path.join(self.tmp_html_dir, "readme.m.html")
            readme_text = []
            start = '<h1 class="title"><span class="name">readme</span> module</h1>'
            stop = '</header>'
            copy = False
            with open(readme_src, "r") as reader:
                for line in reader.readlines():
                    if not copy:
                        if start in line:
                            copy = True
                    else:
                        if stop in line:
                            break
                        readme_text.append(line)
        except IOError as e:
            print "IOError: %s" % e
            readme_text = ''

        logger.debug("_make_main_index: shutil.copy('%s', '%s')", src, dst)
        shutil.copy(src, dst)

        bonus_text = """
First create a TkContext (more details <a href="tkcontext.m.html#sparktk.tkcontext.TkContext.__init__">here</a>).
<br>
<br>
The sparktk Python API centers around the TkContext object. This object holds the session's requisite SparkContext
object in order to work with Spark. It also provides the entry point to the main APIs.
<br>
<br>
        """
        joined = ''.join(readme_text)
        joined = joined.replace('Create a TkContext', bonus_text)
        body = self.main_index_body % joined

        # post-processes the copied index.html
        def html_main_index_processor(full_name, reader, writer):
            COPY = 0
            SKIP = 1
            state = COPY
            for line in reader.readlines():
                if state == COPY:
                    if line.lstrip().startswith("<body>"):
                        writer.write(body)
                        state = SKIP
                    else:
                        writer.write(line)
                elif state == SKIP and line.lstrip().startswith("</body>"):
                        state = COPY


        process_file(dst, html_main_index_processor)

    def _patch_main_tkcontext_html(self):
        """
        Patches up the tkcontext.html for the main API section

        Expects the file to be already cleansed of "show source" buttons
        """

        def extra(line):
            """Some extra line processing for the tkcontext.html to do during one of the 'remove' passes below"""

            # Don't need to show the validate method publically --it's really only used internally
            if '<li class="mono"><a href="#sparktk.tkcontext.TkContext.validate">validate</a></li>' in line:
                return ''

            # Remove the "Classes" header --unneeded and distracting for the this page
            if '<li class="set"><h3><a href="#header-classes">Classes</a></h3>' in line:
                return '<li class="set"><h3></h3>'
            if '<h2 class="section-title" id="header-classes">Classes</h2>' in line:
                return ''

            # patch the index on the left to include links to all the public attributes on the Context
            if '<li class="mono"><a href="#sparktk.tkcontext.TkContext.__init__">__init__</a></li>' in line:
                return '\n        '.join([
                    line,
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.agg">agg</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.dicom">dicom</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.examples">examples</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.frame">frame</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.graph">graph</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.load">load</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.models">models</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.sc">sc</a></li>',
                    '<li class="mono"><a href="#sparktk.tkcontext.TkContext.sql_context">sql_context</a></li>',
                ])
            if '<li class="mono"><a href="#sparktk.tkcontext.TkContext.load">load</a></li>' in line:
                # this line is covered by the previous edit.  "load" is actually provided by default, but since
                # we want to alphabetize the index, this line would appear out of place, so removing it
                return ''

            return line

        dst = os.path.join(self.html_dir, "tkcontext.m.html")

        # remove some of the headers and sections that would confuse landing page user

        # don't need to show the validate method
        process_file(dst, self._get_tag_remover('<h3>Static methods</h3>', 'div', extra_line_processor=extra))

        # don't need to show awkward inheritance display
        process_file(dst, self._get_tag_remover('<h3>Ancestors (in MRO)</h3>', 'ul'))

        # don't need to have a Class variables label
        process_file(dst, self._get_tag_remover('<h3>Class variables</h3>', 'div'))


    def _patch_full_index_html(self):
        # put back reference to main index in the full index
        # needs to happen AFTER the main index is created

        back_reference_html = """
        (Note: This is documentation for the complete sparktk python package. For general usage, see
        <a href="../../index.html">the main Python APIs documentation</a>)
        <br>
        """

        def full_html_index_processor(full_name, reader, writer):
            found = False
            for line in reader.readlines():
                writer.write(line)
                if not found and '<section id="section-items">' in line:
                    writer.write(back_reference_html)
                    found = True

        dst = os.path.join(self.tmp_html_dir, "sparktk/index.html")
        process_file(dst, full_html_index_processor)

    @staticmethod
    def _get_show_source_remover(extra_line_processor=None):
        return MainApiDocs._get_tag_remover(tag_start_tell='<p class="source_link">',
                                            extra_line_processor=extra_line_processor)

    @staticmethod
    def _get_tag_remover(tag_start_tell, tag_name='div', extra_line_processor=None):
        """
        Creates a method which will skip chunks of html designated by the tag_name, immediately after a 'tell'

        :param tag_start_tell: this is a snippet of a line which should be skipped and marks the start of
         the following html to skip
        :param tag_name: the name of the html tag that marks a block to remove
        :param extra_line_processor: if provided, this processor will execute on every line that is not skipped.  The
         output of this function replaces the line.


        Example
        -------

        HTML that has "Static methods" that we want to remove:

        blah blah
        </div>

        <h3>Static methods</h3>
        <div class="item">
          <div class="name def" id="sparktk.tkcontext.TkContext.validate">
            <p>def <span class="ident">validate</span>(</p><p>tc, arg_name=&#39;tc&#39;)</p>
          </div>
        ...
        </div>

        <div class="newthing">
        ...
        </div>

        >>> x = MainApiDocs._get_tag_remover(tag_start_tell='<h3>Static methods</h3>',
        ...                                  tag_name='div')


        Yields HTML:

        blah blah
        </div>

        <div class="newthing">
        ...
        </div>

        """

        extra = extra_line_processor
        tell = tag_start_tell
        tag = tag_name
        if isinstance(tell, basestring):
            tell = [tell]
        elif not isinstance(tell, list):
            raise ValueError("tag_start_tell must be a string of a list of strings, got %s" % type(tag_start_tell))

        def _tag_remover(full_name, reader, writer):
            COPY = 0
            SKIP = 1
            SKIP_TAGS = 2
            GET_FIRST_TAG = 3

            state = COPY
            start_tag = '<' + tag
            start_tag_space = start_tag + ' '
            start_tag_closed = start_tag + '>'
            end_tag = '</' + tag + '>'
            tag_count = 0
            line_count = -1
            for line in reader.readlines():
                line_count += 1
                if state == GET_FIRST_TAG:
                    if not line.strip():
                        continue
                    #print "GET_FIRST_TAG (#%s): %s" % (line_count, line)
                    assert line.lstrip().startswith(start_tag), "bad line %s" % line
                    state = SKIP_TAGS
                    # fall through
                if state == SKIP_TAGS:
                    #print "SKIP_TAGS (#%s): %s" % (line_count, line)
                    num_tags = line.count(start_tag_space) + line.count(start_tag_closed)
                    num_tag_nots = line.count(end_tag)
                    tag_count += (num_tags - num_tag_nots)
                    if tag_count <= 0:
                        #print "SKIP_TAGS: div_count <= 0, back to copy (#%s): %s" % (lie_count, line)
                        state = COPY
                    continue
                elif state == SKIP:
                    continue
                else:
                    for t in tell:
                        if t in line:
                            # start skip divs
                            #print "ELSE (#%s): '%s' in '%s'" % (line_count, t, line)
                            state = GET_FIRST_TAG
                            break
                    if state == GET_FIRST_TAG:
                        continue

                processed_line = line if not extra else extra(line)

                writer.write(processed_line)

        return _tag_remover

    def _post_process_for_main(self, path):
        """walks through a directory (the one holding the main API htmls) and cleans up (more removes clutter)"""

        # Simplify header to not have the full module name
        header_pattern = re.compile('<span class="name">sparktk.*\.(\w+)</span>')

        def header_rework(line):
            if line.lstrip().startswith('<h1 class="title">'):
                m = header_pattern.search(line)
                if m:
                    line = '<h1 class="title"><span class="name">sparktk</span> %s</h1>' % m.groups()[0]
            return line

        # use remover to remove all the show_source buttons
        processor = self._get_show_source_remover(header_rework)

        # go!
        walk_path(path, '.html', processor)

def remove_show_source(path, extra_line_processor=None):
    """
    public module utility - make it easy to remove 'show_source' buttons from any file
    """
    remover = MainApiDocs._get_show_source_remover(extra_line_processor=extra_line_processor)
    process_file(path, remover)

##############################################################################

def main():
    script_name = os.path.basename(__file__)
    usage = "Usage: %s <-html=HTML_DIR|-py=PY_DIR> [-d]" % script_name

    if len(sys.argv) < 2:
        raise RuntimeError(usage)

    option = sys.argv[1]
    html_flag = '-html='
    py_flag = '-py='
    if '-d' in sys.argv:
        # -d turns on debug logging
        line_format = "[%s] %s" % (script_name, '%(asctime)s|%(levelname)-5s|%(message)s')
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.name = logger.name
        formatter = logging.Formatter(line_format, '%y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if option.startswith(html_flag):
        value = option[len(html_flag):]
        html_dir = os.path.abspath(value)
        print "[%s] processing HTML at %s" % (script_name, html_dir)
        post_process_html(html_dir)
        MainApiDocs(html_dir)

    elif option.startswith(py_flag):
        value = option[len(py_flag):]
        py_dir = os.path.abspath(value)
        print "[%s] processing Python at %s" % (script_name, py_dir)
        pre_process_py(py_dir)
    else:
        raise RuntimeError(usage)


if __name__ == "__main__":
    main()
