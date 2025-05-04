// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded "><a href="installation.html"><strong aria-hidden="true">1.</strong> Overview</a></li><li class="chapter-item expanded "><a href="contributing.html"><strong aria-hidden="true">2.</strong> Contributing</a></li><li class="chapter-item expanded "><a href="setup_helpers.html"><strong aria-hidden="true">3.</strong> Setup helper programs</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item expanded "><a href="setup/change_ggg_files.html"><strong aria-hidden="true">3.1.</strong> change_ggg_files</a></li><li class="chapter-item expanded "><a href="setup/list_spectra.html"><strong aria-hidden="true">3.2.</strong> list_spectra</a></li></ol></li><li class="chapter-item expanded "><a href="post_processing.html"><strong aria-hidden="true">4.</strong> Post processing programs</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item expanded "><a href="postproc/collate_tccon_results.html"><strong aria-hidden="true">4.1.</strong> collate_tccon_results</a></li><li class="chapter-item expanded "><a href="postproc/apply_tccon_airmass_correction.html"><strong aria-hidden="true">4.2.</strong> apply_tccon_airmass_correction</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="postproc/corrections/airmass_correction_file.html"><strong aria-hidden="true">4.2.1.</strong> Correction file format</a></li></ol></li><li class="chapter-item expanded "><a href="postproc/apply_tccon_insitu_correction.html"><strong aria-hidden="true">4.3.</strong> apply_tccon_insitu_correction</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="postproc/corrections/insitu_correction_file.html"><strong aria-hidden="true">4.3.1.</strong> Correction file format</a></li></ol></li><li class="chapter-item expanded "><a href="postproc/add_nc_flags.html"><strong aria-hidden="true">4.4.</strong> add_nc_flags</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="postproc/add_nc_flags_toml.html"><strong aria-hidden="true">4.4.1.</strong> TOML file format</a></li></ol></li><li class="chapter-item expanded "><a href="postproc/write_public_netcdf.html"><strong aria-hidden="true">4.5.</strong> write_public_netcdf</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="postproc/write_public_netcdf/configuration.html"><strong aria-hidden="true">4.5.1.</strong> Configuration</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="postproc/write_public_netcdf/aux_vars.html"><strong aria-hidden="true">4.5.1.1.</strong> Auxiliary variables</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/comp_vars.html"><strong aria-hidden="true">4.5.1.2.</strong> Computed variables</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/xgas_discovery.html"><strong aria-hidden="true">4.5.1.3.</strong> Xgas discovery</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/explicit_xgases.html"><strong aria-hidden="true">4.5.1.4.</strong> Explicitly specified Xgases</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/gas_proper_names.html"><strong aria-hidden="true">4.5.1.5.</strong> Gas proper names</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/includes.html"><strong aria-hidden="true">4.5.1.6.</strong> Includes</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/defaults.html"><strong aria-hidden="true">4.5.1.7.</strong> Defaults</a></li><li class="chapter-item "><a href="postproc/write_public_netcdf/debugging_tips.html"><strong aria-hidden="true">4.5.1.8.</strong> Debugging tips</a></li></ol></li></ol></li></ol></li><li class="chapter-item expanded "><a href="other_utils.html"><strong aria-hidden="true">5.</strong> Other utility programs</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item expanded "><a href="other/bin2nc.html"><strong aria-hidden="true">5.1.</strong> bin2nc</a></li><li class="chapter-item expanded "><a href="other/query_output.html"><strong aria-hidden="true">5.2.</strong> query_output</a></li><li class="chapter-item expanded "><a href="other/strip_header.html"><strong aria-hidden="true">5.3.</strong> strip_header</a></li></ol></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
