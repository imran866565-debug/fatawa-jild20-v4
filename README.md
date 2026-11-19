# Fatawa Razviya — Jild 20 tashreeh v4 merge

Merge workspace: 12 leaf chunk files of the easy-Urdu tashreeh v4 for Fatawa Razviya Jild 20 (volume_id 44; 255 items, 530 pages) are merged into `jild-20.json` by GitHub Actions (`merge_jild20_v4.py`), then published as a v1.0 release asset.

Item order reproduces the v3 merged file exactly (first jild20-pesh-lafz, last jild20-maakhiz-zamima). All 12 leaf chunks carry start_page == min(pages). No renames at merge — the 5 print-error renames were already applied in the leaves — and the merge joins 3 simple duplicate-id pairs (masala-23, 192, 242) plus the cross-id masala-83 (bare-id head), the 13-part masala-203 risala mega-combine, and masala-293 (head + safia-tail), then stable-sorts by start_page, matching the v3 merged file. No page markers are present in the chunks.
