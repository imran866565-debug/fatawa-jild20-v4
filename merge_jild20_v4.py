#!/usr/bin/env python3
"""Merge Fatawa Razviya Jild 20 easy-Urdu tashreeh v4 chunks into jild-20.json.

Reads the 12 leaf chunk files listed in chunks-manifest-v4-jild20.json,
concatenates them in order 01 -> 02 -> ... -> 12, JOINS the 3 simple
duplicate-id pairs (masala-23, 192, 242) and the 3 cross-id groups
(masala-83: bare-id head + masala-83 tail; masala-203: head + 12
tanbih/tadhyeel tails; masala-293: head + safia-tail) into single
items, STABLE SORTS by start_page (mirroring the v3 merge), derives
start_page = min(pages) where the field is absent, normalises page
markers, validates, and writes {"masail": [...]} to jild-20.json.

ORDER -- chunk order is the order, then a stable sort by start_page:
  The v3 merged file ~/workspace/fatawa-app/content/tashreeh/jild-20.json
  has ID order == stable sort by start_page of the concatenation
  (verified locally by the coordinator: the v3 order IS the stable
  sort, including its tied start_pages). Do NOT sort or reorder before
  the joins. Within each chunk, items keep file order.
  CHUNK_ORDER is an explicit 12-leaf list.

JOINS: parts are found BY ID anywhere in the list (masala-140-style
  non-consecutiveness is handled the same way as Jild 19). Parts must
  be in ascending page order. Sawal modes: "first" (23, 192, 242 —
  the single non-empty sawal, in the first part), "head_only" (83,
  203, 293 — the head's sawal; all tails have empty sawals). Titles:
  the joined title is the FIRST/HEAD part's title, asserted against
  the embedded merged-v3 title (V3_TITLES). title_must_match is
  deliberately NOT used: the two halves' titles differ by design in
  the v3 chunks. masala-23 [125-134, 135-138], masala-192 [367-368,
  369], masala-242 [467-468, 469-471].
CROSS-ID JOINS (3):
  masala-83: c03 bare-id head jild20-83 [224-226] (sawal) + c04
    jild20-masala-83 [227] (sawal empty) -> ONE item jild20-masala-83;
    title from the HEAD (== merged-v3 title, 'درانتی سے ذبح کیا ہوا
    شکار'); the rename is effected by the combine. Net -1 item.
  masala-203 RISALA MEGA-COMBINE (13 parts): c07 head
    jild20-masala-203 [382-418] + c08's 12 tail items tanbih-15..25 +
    tadhyeel [419-435] -> ONE item [382-435], 54 pages; title from
    the head; sawal = head only. The risala ھادی الاضحیۃ ends p.435;
    masail 204-292 (pp.436-508) are separate items — NO fold.
  masala-293: c10 head jild20-masala-293 [509,511-558] + c11
    jild20-safia-tail [559-561] -> ONE item; title from the head;
    sawal = head only. p.510 is a source gap.
  After the joins there are 255 items and 255 unique ids — the
  duplicate-id assertion runs AFTER the joins, not before.
  NO renames at merge: the 5 print-error renames (masala-248,
  masala-184, masala-235, masala-237, p442) were already applied in
  the chunk files; the merge only asserts they are present in the
  leaves and in merged v3. NO folds. All chunks carry start_page ==
  min(pages) (0 derived).
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = "chunks-manifest-v4-jild20.json"

# Concatenation order: chunk files 01 -> 18, in order.
CHUNK_ORDER = ["jild-20-chunk-01.json", "jild-20-chunk-02.json", "jild-20-chunk-03.json", "jild-20-chunk-04.json", "jild-20-chunk-05.json", "jild-20-chunk-06.json", "jild-20-chunk-07.json", "jild-20-chunk-08.json", "jild-20-chunk-09.json", "jild-20-chunk-10.json", "jild-20-chunk-11.json", "jild-20-chunk-12.json"]

EXPECTED_ITEMS = 255
EXPECTED_PAGES = 530
EXPECTED_MIN_PAGE = 5
EXPECTED_MAX_PAGE = 629
EXPECTED_VOLUME_IDS = {44}
EXPECTED_FIRST_ID = "jild20-pesh-lafz"
EXPECTED_LAST_ID = "jild20-maakhiz-zamima"

# v3 ID sequence, verbatim from content/tashreeh/jild-20.json.
V3_IDS = ["jild20-pesh-lafz", "jild20-masala-1", "jild20-masala-2", "jild20-masala-3", "jild20-masala-4", "jild20-masala-5", "jild20-masala-6", "jild20-masala-7", "jild20-masala-8-12", "jild20-masala-13", "jild20-masala-14", "jild20-masala-15", "jild20-masala-16", "jild20-masala-17", "jild20-masala-18", "jild20-masala-19", "jild20-masala-20", "jild20-masala-21", "jild20-masala-22", "jild20-masala-23", "jild20-masala-24", "jild20-masala-25", "jild20-masala-26", "jild20-masala-27", "jild20-masala-28", "jild20-masala-29-30", "jild20-masala-31", "jild20-masala-32", "jild20-masala-33", "jild20-masala-34-38", "jild20-masala-39", "jild20-masala-40", "jild20-masala-41", "jild20-masala-42", "jild20-masala-43", "jild20-44", "jild20-45", "jild20-46-49", "jild20-50-53", "jild20-54", "jild20-55", "jild20-56", "jild20-57", "jild20-58-61", "jild20-62-65", "jild20-66", "jild20-67", "jild20-68", "jild20-69", "jild20-70", "jild20-71", "jild20-72", "jild20-73", "jild20-74", "jild20-75", "jild20-76", "jild20-77", "jild20-78", "jild20-79-80", "jild20-81", "jild20-82", "jild20-masala-83", "jild20-masala-84-85", "jild20-masala-86", "jild20-masala-87", "jild20-masala-88", "jild20-masala-89", "jild20-masala-90", "jild20-masala-91", "jild20-masala-92", "jild20-masala-93", "jild20-masala-94", "jild20-masala-95", "jild20-masala-96", "jild20-masala-97", "jild20-masala-98", "jild20-masala-99", "jild20-masala-100-101", "jild20-masala-102", "jild20-masala-103", "jild20-masala-104", "jild20-masala-105-107", "jild20-masala-108", "jild20-masala-109", "jild20-masala-110", "jild20-masala-111", "jild20-masala-112", "jild20-masala-113", "jild20-masala-114", "jild20-masala-115", "jild20-masala-116", "jild20-masala-117", "jild20-masala-118", "jild20-masala-119-120", "jild20-masala-121", "jild20-masala-122", "jild20-masala-123", "jild20-masala-124", "jild20-masala-125", "jild20-masala-126", "jild20-masala-127", "jild20-masala-128-131", "jild20-masala-132", "jild20-masala-133", "jild20-masala-134", "jild20-masala-135", "jild20-masala-136", "jild20-masala-137", "jild20-masala-138", "jild20-masala-139", "jild20-masala-140", "jild20-masala-141", "jild20-masala-142", "jild20-masala-143", "jild20-masala-144-145", "jild20-masala-146", "jild20-masala-147", "jild20-masala-148", "jild20-masala-149", "jild20-masala-150", "jild20-masala-151", "jild20-masala-152", "jild20-masala-153", "jild20-masala-154", "jild20-masala-155", "jild20-masala-156", "jild20-masala-157", "jild20-masala-158", "jild20-masala-159", "jild20-masala-160-161", "jild20-masala-162", "jild20-masala-163-164", "jild20-masala-165", "jild20-masala-166-167", "jild20-masala-168", "jild20-masala-169", "jild20-masala-170", "jild20-masala-171", "jild20-masala-172", "jild20-masala-173", "jild20-masala-174-176", "jild20-masala-177", "jild20-masala-178", "jild20-masala-179", "jild20-masala-180", "jild20-masala-181", "jild20-masala-182", "jild20-masala-183", "jild20-masala-184", "jild20-masala-185", "jild20-masala-186", "jild20-masala-187", "jild20-masala-188", "jild20-masala-189", "jild20-masala-190", "jild20-masala-191", "jild20-masala-192", "jild20-masala-193", "jild20-masala-194", "jild20-masala-195", "jild20-masala-196", "jild20-masala-197-198", "jild20-masala-199", "jild20-masala-200", "jild20-masala-201", "jild20-masala-202", "jild20-masala-203", "jild20-masala-204", "jild20-p442", "jild20-masala-205-206", "jild20-masala-207", "jild20-masala-208", "jild20-masala-209", "jild20-masala-210-212", "jild20-masala-213", "jild20-masala-214-215", "jild20-masala-216", "jild20-masala-217-221", "jild20-masala-222", "jild20-masala-223", "jild20-masala-224", "jild20-masala-225", "jild20-masala-226", "jild20-masala-227", "jild20-masala-228", "jild20-masala-229", "jild20-masala-230", "jild20-masala-231", "jild20-masala-232", "jild20-masala-233", "jild20-masala-234", "jild20-masala-235", "jild20-masala-236", "jild20-masala-237", "jild20-masala-238", "jild20-masala-239", "jild20-masala-240", "jild20-masala-241", "jild20-masala-242", "jild20-masala-243", "jild20-masala-244", "jild20-masala-245", "jild20-masala-246", "jild20-masala-247", "jild20-masala-248", "jild20-masala-249-250", "jild20-masala-251-254", "jild20-masala-255", "jild20-masala-256", "jild20-masala-257", "jild20-masala-258-264", "jild20-masala-265-269", "jild20-masala-270", "jild20-masala-271", "jild20-masala-272", "jild20-masala-273-274", "jild20-masala-275", "jild20-masala-276-279", "jild20-masala-280", "jild20-masala-281", "jild20-masala-282", "jild20-masala-283", "jild20-masala-284", "jild20-masala-285", "jild20-masala-286", "jild20-masala-287", "jild20-masala-288", "jild20-masala-289", "jild20-masala-290", "jild20-masala-291", "jild20-masala-292", "jild20-masala-293", "jild20-masala-294", "jild20-masala-295", "jild20-masala-296", "jild20-masala-297", "jild20-masala-298-299", "jild20-masala-300-310", "jild20-masala-311-314", "jild20-masala-315", "jild20-masala-316", "jild20-masala-317-319", "jild20-masala-320", "jild20-masala-321", "jild20-masala-322", "jild20-masala-323", "jild20-masala-324-325", "jild20-masala-326", "jild20-masala-327", "jild20-masala-328-329", "jild20-masala-330", "jild20-masala-331-333", "jild20-masala-334", "jild20-maakhiz", "jild20-maakhiz-zamima"]

# v3 pages per item id, verbatim from content/tashreeh/jild-20.json.
V3_PAGES = {
    "jild20-pesh-lafz": [5, 6, 7, 8],
    "jild20-masala-1": [95, 96, 97, 98],
    "jild20-masala-2": [98, 99],
    "jild20-masala-3": [100, 101, 102],
    "jild20-masala-4": [102, 103, 104],
    "jild20-masala-5": [104, 105, 106, 107, 108],
    "jild20-masala-6": [108, 109, 110],
    "jild20-masala-7": [110, 111],
    "jild20-masala-8-12": [111, 112, 113],
    "jild20-masala-13": [114],
    "jild20-masala-14": [114, 115],
    "jild20-masala-15": [115, 116],
    "jild20-masala-16": [116, 117],
    "jild20-masala-17": [117, 118, 119],
    "jild20-masala-18": [119],
    "jild20-masala-19": [119, 120],
    "jild20-masala-20": [120, 121],
    "jild20-masala-21": [122, 123],
    "jild20-masala-22": [124, 125],
    "jild20-masala-23": [125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138],
    "jild20-masala-24": [138, 139, 140],
    "jild20-masala-25": [140, 141, 142, 143, 144],
    "jild20-masala-26": [145, 146, 147],
    "jild20-masala-27": [147, 148, 149],
    "jild20-masala-28": [149, 150, 151],
    "jild20-masala-29-30": [151, 152, 153, 154, 155, 156],
    "jild20-masala-31": [157, 158, 159],
    "jild20-masala-32": [159, 160, 161, 162],
    "jild20-masala-33": [162, 163, 164],
    "jild20-masala-34-38": [164, 165, 166, 167, 168, 169],
    "jild20-masala-39": [169, 170, 171],
    "jild20-masala-40": [171],
    "jild20-masala-41": [171, 172],
    "jild20-masala-42": [172, 173, 174],
    "jild20-masala-43": [174],
    "jild20-44": [175, 176],
    "jild20-45": [177],
    "jild20-46-49": [179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192],
    "jild20-50-53": [193, 194, 195, 196, 197],
    "jild20-54": [197, 198, 199, 200, 201, 202],
    "jild20-55": [202, 203],
    "jild20-56": [203, 204],
    "jild20-57": [204, 205],
    "jild20-58-61": [206],
    "jild20-62-65": [206, 207, 208],
    "jild20-66": [209],
    "jild20-67": [209],
    "jild20-68": [209, 210],
    "jild20-69": [210],
    "jild20-70": [210, 211],
    "jild20-71": [213, 214, 215],
    "jild20-72": [215],
    "jild20-73": [215, 216],
    "jild20-74": [216, 217, 218],
    "jild20-75": [218, 219],
    "jild20-76": [219, 220],
    "jild20-77": [220, 221],
    "jild20-78": [221, 222],
    "jild20-79-80": [222, 223],
    "jild20-81": [223],
    "jild20-82": [223, 224],
    "jild20-masala-83": [224, 225, 226, 227],
    "jild20-masala-84-85": [227, 228],
    "jild20-masala-86": [228],
    "jild20-masala-87": [228, 229, 230, 231, 232, 233],
    "jild20-masala-88": [233, 234],
    "jild20-masala-89": [234, 235, 236, 237, 238, 239, 240],
    "jild20-masala-90": [240, 241],
    "jild20-masala-91": [241],
    "jild20-masala-92": [242],
    "jild20-masala-93": [242, 243, 244],
    "jild20-masala-94": [244, 245, 246],
    "jild20-masala-95": [246, 247, 248, 249],
    "jild20-masala-96": [249],
    "jild20-masala-97": [249, 250],
    "jild20-masala-98": [249, 250],
    "jild20-masala-99": [250],
    "jild20-masala-100-101": [250, 251],
    "jild20-masala-102": [251],
    "jild20-masala-103": [251],
    "jild20-masala-104": [251],
    "jild20-masala-105-107": [251, 252, 253],
    "jild20-masala-108": [253],
    "jild20-masala-109": [253, 254],
    "jild20-masala-110": [254],
    "jild20-masala-111": [254, 255],
    "jild20-masala-112": [255, 256],
    "jild20-masala-113": [256, 257, 258],
    "jild20-masala-114": [258, 259],
    "jild20-masala-115": [259],
    "jild20-masala-116": [260, 261],
    "jild20-masala-117": [261, 262],
    "jild20-masala-118": [262],
    "jild20-masala-119-120": [262, 263],
    "jild20-masala-121": [263, 264],
    "jild20-masala-122": [264, 265],
    "jild20-masala-123": [265, 266],
    "jild20-masala-124": [266, 267],
    "jild20-masala-125": [269, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280],
    "jild20-masala-126": [280],
    "jild20-masala-127": [280, 281],
    "jild20-masala-128-131": [281, 282, 283, 284],
    "jild20-masala-132": [284, 285, 286],
    "jild20-masala-133": [287, 288, 289],
    "jild20-masala-134": [289, 290, 291],
    "jild20-masala-135": [291],
    "jild20-masala-136": [291, 292, 293, 294],
    "jild20-masala-137": [294, 295],
    "jild20-masala-138": [296],
    "jild20-masala-139": [296],
    "jild20-masala-140": [296, 297],
    "jild20-masala-141": [297],
    "jild20-masala-142": [297, 298, 299, 300, 301, 302, 303, 304],
    "jild20-masala-143": [305],
    "jild20-masala-144-145": [305, 306],
    "jild20-masala-146": [306],
    "jild20-masala-147": [306, 307],
    "jild20-masala-148": [307, 308, 309],
    "jild20-masala-149": [309, 310, 311],
    "jild20-masala-150": [311, 312],
    "jild20-masala-151": [312, 313],
    "jild20-masala-152": [313],
    "jild20-masala-153": [313, 314, 315, 316, 317, 318],
    "jild20-masala-154": [319, 320],
    "jild20-masala-155": [320, 321],
    "jild20-masala-156": [321],
    "jild20-masala-157": [321],
    "jild20-masala-158": [321, 322],
    "jild20-masala-159": [322],
    "jild20-masala-160-161": [322, 323],
    "jild20-masala-162": [323],
    "jild20-masala-163-164": [323, 324],
    "jild20-masala-165": [324],
    "jild20-masala-166-167": [324, 325, 326, 327, 328, 329, 330, 331, 332, 333],
    "jild20-masala-168": [333, 334],
    "jild20-masala-169": [334, 335],
    "jild20-masala-170": [335],
    "jild20-masala-171": [335, 336],
    "jild20-masala-172": [337],
    "jild20-masala-173": [337, 338, 339, 340],
    "jild20-masala-174-176": [340, 341],
    "jild20-masala-177": [341],
    "jild20-masala-178": [342, 343],
    "jild20-masala-179": [343],
    "jild20-masala-180": [343, 344],
    "jild20-masala-181": [344, 345],
    "jild20-masala-182": [345, 346, 347],
    "jild20-masala-183": [348, 349],
    "jild20-masala-184": [349, 350],
    "jild20-masala-185": [350, 351, 352],
    "jild20-masala-186": [353, 355],
    "jild20-masala-187": [355],
    "jild20-masala-188": [355, 356, 357, 358, 359, 360, 361],
    "jild20-masala-189": [361],
    "jild20-masala-190": [361, 362],
    "jild20-masala-191": [362, 363, 364, 365, 366, 367],
    "jild20-masala-192": [367, 368, 369],
    "jild20-masala-193": [369, 370],
    "jild20-masala-194": [370],
    "jild20-masala-195": [370, 371],
    "jild20-masala-196": [371, 372],
    "jild20-masala-197-198": [372, 373],
    "jild20-masala-199": [373, 374, 375, 376, 377, 378],
    "jild20-masala-200": [378, 379],
    "jild20-masala-201": [379, 380, 381],
    "jild20-masala-202": [381],
    "jild20-masala-203": [382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435],
    "jild20-masala-204": [436, 437, 438, 439, 440, 441, 442],
    "jild20-p442": [442],
    "jild20-masala-205-206": [442, 443, 444],
    "jild20-masala-207": [444],
    "jild20-masala-208": [444],
    "jild20-masala-209": [444, 445],
    "jild20-masala-210-212": [446],
    "jild20-masala-213": [446, 447],
    "jild20-masala-214-215": [447, 448],
    "jild20-masala-216": [448, 449],
    "jild20-masala-217-221": [449],
    "jild20-masala-222": [449, 450],
    "jild20-masala-223": [450, 451],
    "jild20-masala-224": [451],
    "jild20-masala-225": [451, 452],
    "jild20-masala-226": [454],
    "jild20-masala-227": [454, 455],
    "jild20-masala-228": [455, 456],
    "jild20-masala-229": [456],
    "jild20-masala-230": [456, 457],
    "jild20-masala-231": [457],
    "jild20-masala-232": [457, 458],
    "jild20-masala-233": [458],
    "jild20-masala-234": [458, 459],
    "jild20-masala-235": [459],
    "jild20-masala-236": [459, 460],
    "jild20-masala-237": [460, 461],
    "jild20-masala-238": [461, 462],
    "jild20-masala-239": [462, 463, 464, 465],
    "jild20-masala-240": [465, 466],
    "jild20-masala-241": [466, 467],
    "jild20-masala-242": [467, 468, 469, 470, 471],
    "jild20-masala-243": [472],
    "jild20-masala-244": [472, 473],
    "jild20-masala-245": [473, 474],
    "jild20-masala-246": [474, 475],
    "jild20-masala-247": [475, 476, 477],
    "jild20-masala-248": [477, 478],
    "jild20-masala-249-250": [478, 479],
    "jild20-masala-251-254": [478, 479, 480],
    "jild20-masala-255": [480, 481],
    "jild20-masala-256": [481],
    "jild20-masala-257": [481, 482],
    "jild20-masala-258-264": [482, 483, 484],
    "jild20-masala-265-269": [484, 485],
    "jild20-masala-270": [485, 486],
    "jild20-masala-271": [486, 487],
    "jild20-masala-272": [487, 488],
    "jild20-masala-273-274": [488, 489],
    "jild20-masala-275": [489, 490, 491],
    "jild20-masala-276-279": [491, 492, 493, 494, 495, 496, 497],
    "jild20-masala-280": [497, 498],
    "jild20-masala-281": [498, 499],
    "jild20-masala-282": [499, 500, 501],
    "jild20-masala-283": [501, 502],
    "jild20-masala-284": [502, 503],
    "jild20-masala-285": [503],
    "jild20-masala-286": [504],
    "jild20-masala-287": [504],
    "jild20-masala-288": [504, 505],
    "jild20-masala-289": [505, 506],
    "jild20-masala-290": [506, 507],
    "jild20-masala-291": [507],
    "jild20-masala-292": [507, 508],
    "jild20-masala-293": [509, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561],
    "jild20-masala-294": [561],
    "jild20-masala-295": [561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579],
    "jild20-masala-296": [581],
    "jild20-masala-297": [581, 582, 583],
    "jild20-masala-298-299": [583, 584],
    "jild20-masala-300-310": [584, 585, 586],
    "jild20-masala-311-314": [586, 587],
    "jild20-masala-315": [587],
    "jild20-masala-316": [587, 588],
    "jild20-masala-317-319": [588, 589, 590],
    "jild20-masala-320": [590],
    "jild20-masala-321": [590],
    "jild20-masala-322": [590, 591],
    "jild20-masala-323": [591, 592],
    "jild20-masala-324-325": [592, 593],
    "jild20-masala-326": [592, 593],
    "jild20-masala-327": [593],
    "jild20-masala-328-329": [593, 594],
    "jild20-masala-330": [594],
    "jild20-masala-331-333": [594, 595],
    "jild20-masala-334": [595, 596, 597],
    "jild20-maakhiz": [598, 599, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622],
    "jild20-maakhiz-zamima": [623, 624, 625, 626, 627, 628, 629],
}

# The Urdu word for "page" is safha; the canonical marker is [safha N].
# Lenticular brackets are U+3010 (open) and U+3011 (close); the short
# form is "[safha-initial N]" with the single-letter abbreviation.
# All three patterns are built from escapes so this file never contains
# a literal lenticular-bracket pair.
SAFHA = "\u0635\u0641\u062d\u06c1"  # the Urdu word for page
RE_LENTICULAR = re.compile("\u3010" + SAFHA + " (\\d+)\u3011")
RE_SHORT = re.compile("\\[\u0635 (\\d+)\\]")
RE_FULL = re.compile("\\[" + SAFHA + " \\d+\\]")
LENTICULAR_PREFIX = "\u3010" + SAFHA  # for the "none remain" check
SHORT_PREFIX_RE = re.compile("\\[\u0635 \\d+\\]")

# Canonical output key order for every item (the JSON contract).
ITEM_KEYS = ["id", "title", "volume_id", "pages", "start_page",
             "tashreeh_sawal", "tashreeh_jawab", "notes"]


def normalise_markers(text):
    """Normalise page markers in one string. Returns (new_text, counts)."""
    counts = {"lenticular": 0, "short": 0, "full_before": 0}
    counts["full_before"] = len(RE_FULL.findall(text))
    text, n1 = RE_LENTICULAR.subn("[" + SAFHA + " \\1]", text)
    counts["lenticular"] = n1
    text, n2 = RE_SHORT.subn("[" + SAFHA + " \\1]", text)
    counts["short"] = n2
    return text, counts


def normalise_item(item):
    """Normalise markers in every string field of one item (recursively
    through lists/dicts). Returns per-form counts for this item."""
    totals = {"lenticular": 0, "short": 0, "full_before": 0}

    def walk(value):
        if isinstance(value, str):
            new, c = normalise_markers(value)
            for k in totals:
                totals[k] += c[k]
            return new
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    for key in list(item.keys()):
        item[key] = walk(item[key])
    return totals


def main():
    assert len(V3_IDS) == EXPECTED_ITEMS, "embedded V3_IDS length mismatch"
    assert set(V3_PAGES) == set(V3_IDS), "embedded V3_PAGES keys != V3_IDS"
    assert sum(len(v) for v in V3_PAGES.values()) >= EXPECTED_PAGES

    manifest = json.load(open(os.path.join(BASE, MANIFEST), encoding="utf-8"))
    entries = manifest["chunks"]
    assert len(entries) == 12, f"expected 12 manifest entries, got {len(entries)}"
    assert all(e.get("status") == "done" for e in entries), "not all chunks done"
    by_file = {e["file"]: e for e in entries}
    assert set(by_file) == set(CHUNK_ORDER), (
        f"manifest files != CHUNK_ORDER: "
        f"missing={set(CHUNK_ORDER) - set(by_file)} "
        f"extra={set(by_file) - set(CHUNK_ORDER)}"
    )

    all_items = []
    derived = []
    for fname in CHUNK_ORDER:
        path = os.path.join(BASE, fname)
        d = json.load(open(path, encoding="utf-8"))
        items = d["masail"]
        entry = by_file[fname]
        assert len(items) == entry["items"], (
            f"{fname}: {len(items)} items, manifest says {entry['items']}"
        )
        # Marker normalisation, with before/after counts per chunk.
        before = {"lenticular": 0, "short": 0, "full_before": 0}
        for it in items:
            c = normalise_item(it)
            for k in before:
                before[k] += c[k]
        after_full = sum(
            len(RE_FULL.findall(json.dumps(it, ensure_ascii=False)))
            for it in items
        )
        print(
            f"{fname}: {len(items)} items | markers before: "
            f"lenticular={before['lenticular']} short={before['short']} "
            f"full={before['full_before']} | after: full={after_full}"
        )
        assert after_full == (
            before["lenticular"] + before["short"] + before["full_before"]
        ), f"{fname}: marker count changed by normalisation!"
        # Derive start_page = min(pages) where absent (none expected: all chunks carry start_page).
        for it in items:
            if "start_page" not in it:
                it["start_page"] = min(it["pages"])
                derived.append(it["id"])
        all_items.extend(items)

        # SIMPLE JOINS (3): parts found BY ID anywhere. Sawal mode:
    # "first" (the single non-empty sawal, in the first part).
    # CROSS-ID JOINS (3): 83 (bare-id head -> masala-83), 203
    # (13-way mega-combine), 293 (head + safia-tail). Sawal = head.

    def join_consecutive(item_id, expected_parts, seam_checks, sawal_mode="first", title_suffix=None):
        idxs = [i for i, it in enumerate(all_items) if it["id"] == item_id]
        assert len(idxs) == expected_parts, (
            f"{item_id}: expected {expected_parts} parts, found {len(idxs)}"
        )
        parts = [all_items[i] for i in idxs]
        assert [min(p["pages"]) for p in parts] == sorted(min(p["pages"]) for p in parts), (
            f"{item_id}: parts not in page order"
        )
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{item_id}: volume mismatch {vols}"
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{item_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{item_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        if sawal_mode == "first":
            sawals = [p.get("tashreeh_sawal") for p in parts if p.get("tashreeh_sawal")]
            assert len(sawals) <= 1, f"{item_id}: {len(sawals)} non-empty sawals"
            if sawals:
                assert parts[0].get("tashreeh_sawal"), f"{item_id}: non-empty sawal not in first part"
            sawal = sawals[0] if sawals else ""
        else:
            raise AssertionError(f"{item_id}: unknown sawal_mode {sawal_mode}")
        title = parts[0]["title"]
        if title_suffix:
            assert title.endswith(title_suffix), f"{item_id}: first-part title lacks suffix {title_suffix!r}"
            assert all(p["title"].endswith(title_suffix) for p in parts), f"{item_id}: a part title lacks the suffix"
            title = title[: -len(title_suffix)]
        assert title == V3_TITLES[item_id], f"{item_id}: joined title != v3 title"
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[item_id], f"{item_id}: joined pages != v3"
        joined = {
            "id": item_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {item_id}: {expected_parts} parts -> 1 item, {len(pages)} pages")

    def join_cross_id(result_id, head_id, tail_ids, sawal_mode, seam_checks):
        seq = [head_id] + tail_ids
        idxs = [i for i, it in enumerate(all_items) if it["id"] in seq]
        assert [all_items[i]["id"] for i in idxs] == seq, (
            f"{result_id}: cross-id order mismatch"
        )
        parts = [all_items[i] for i in idxs]
        vols = {p.get("volume_id") for p in parts}
        assert vols == EXPECTED_VOLUME_IDS, f"{result_id}: volume mismatch {vols}"
        if sawal_mode == "head_only":
            assert parts[0].get("tashreeh_sawal"), f"{result_id}: head sawal empty"
            assert all(not p.get("tashreeh_sawal") for p in parts[1:]), (
                f"{result_id}: a tail sawal is non-empty"
            )
            sawal = parts[0]["tashreeh_sawal"]
        else:
            raise AssertionError(f"{result_id}: unknown sawal_mode {sawal_mode}")
        for part_no, which, needle in seam_checks:
            txt = parts[part_no].get("tashreeh_jawab") or ""
            if which == "end":
                assert txt.rstrip().endswith(needle), (
                    f"{result_id} part {part_no + 1} end seam mismatch: ...{txt.rstrip()[-60:]!r}"
                )
            else:
                assert txt.lstrip().startswith(needle), (
                    f"{result_id} part {part_no + 1} start seam mismatch: {txt.lstrip()[:60]!r}"
                )
        pages = sorted({p for part in parts for p in part["pages"]})
        assert pages == V3_PAGES[result_id], f"{result_id}: joined pages != v3"
        title = parts[0]["title"]
        assert title == V3_TITLES[result_id], f"{result_id}: joined title != v3 title"
        joined = {
            "id": result_id,
            "title": title,
            "volume_id": parts[0]["volume_id"],
            "pages": pages,
            "start_page": min(pages),
            "tashreeh_sawal": sawal,
            "tashreeh_jawab": " ".join(p["tashreeh_jawab"].strip() for p in parts if p.get("tashreeh_jawab")),
            "notes": [n for p in parts for n in p.get("notes", [])],
        }
        first = idxs[0]
        for i in reversed(idxs[1:]):
            del all_items[i]
        all_items[first] = joined
        print(f"joined {result_id}: {1 + len(tail_ids)} parts -> 1 item, {len(pages)} pages")

    V3_TITLES = {
        "jild20-masala-23": "مسئلہ ۲۳: طلب مواثبت پر شفیع کا حلف یا مشتری کا حلف (نامکمل — جاری ہے)",
        "jild20-masala-192": "مسئلہ ۱۹۲: جائداد اور بیوی کے زیور پر صدقہ فطر و قربانی",
        "jild20-masala-242": "مسئلہ ۲۴۲ — قربانی کی کھال بیچ کر مسجد میں بوریا رکھنا",
        "jild20-masala-83": "درانتی سے ذبح کیا ہوا شکار",
        "jild20-masala-203": "مسئلہ ۲۰۳: رسالہ ھادی الاضحیۃ بالشاۃ الھندیۃ ۱۳۱۴ھ — ہندوستانی بھیڑ کی قربانی (صفحات ۳۸۲ تا ۴۱۸، جاری)",
        "jild20-masala-293": "مسئلہ ۲۹۳: قربانی کی کھال بیچ کر اس کی قیمت مدارس و مساجد وغیرہ کارِ ثواب میں صرف کرنا",
    }

    join_consecutive(
        "jild20-masala-23", 2,
        [
            (0, "end", "ہے اور جواب آگے جاری ہے۔ باکرہ کا مطلب ہے کنواری لڑکی، اور نظیر کا مطلب ہے مثال۔"),
            (1, "start", "یہ مسئلہ ۲۳ کے جواب کی تحقیق کا آخری حصہ ہے۔ شفعہ کا مطلب ہے پڑوسی یا شریک کا یہ"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild20-masala-192", 2,
        [
            (0, "end", " کے اور اس کے عیال کے لیے کافی نہیں، اور اس کے بعد جواب صفحہ ۳۶۸ کے بعد جاری ہے۔"),
            (1, "start", "اس صفحے پر مسئلہ ۱۹۲ کے جواب کا آخری حصہ ہے۔ سب سے پہلے یہ اصول بیان ہوا ہے کہ ج"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_consecutive(
        "jild20-masala-242", 2,
        [
            (0, "end", " کا مطلب ہے اصل چیز — اور گوشت حکم میں بمنزلہ کھال کے ہے، صحیح مذہب میں، ملخصاً۔"),
            (1, "start", "یہ حصہ اسی تحقیق کا اختتام ہے جس میں قربانی کے گوشت اور کھال میں تصرف کا حکم بیا"),
        ],
        sawal_mode="first",
        title_suffix=None,
    )
    join_cross_id(
        "jild20-masala-83",
        "jild20-83",
        ["jild20-masala-83"],
        "head_only",
        [
            (0, "end", "۲۲۷ پر جاری ہے، اس لیے اس حصے کی تشریح یہیں تک ہے، باقی حصہ اگلے چنک میں آئے گا۔"),
            (1, "start", "یہ مسئلہ چنک ۰۳ سے جاری ہے — اس کا سوال اور جواب کا پہلا حصہ پچھلے چنک کے صفحات "),
        ],
    )
    join_cross_id(
        "jild20-masala-203",
        "jild20-masala-203",
        ["jild20-tanbih-15", "jild20-tanbih-16", "jild20-tanbih-17", "jild20-tanbih-18", "jild20-tanbih-19", "jild20-tanbih-20", "jild20-tanbih-21", "jild20-tanbih-22", "jild20-tanbih-23", "jild20-tanbih-24", "jild20-tanbih-25", "jild20-tadhyeel"],
        "head_only",
        [
            (0, "end", "سے نکالا جائے، تساوی کا مطلب ہے برابری، اور معرف کا مطلب ہے جس کی تعریف کی جائے۔"),
            (1, "start", "یہ تنبیہ رسالہ «ہادی الاضحیۃ بالشاۃ الھندیۃ» کی پندرہویں تنبیہ ہے، جو پچھلے حصے "),
            (1, "end", "خلاف خوش اعتمادی کے سوا اور کوئی دلیل نہیں، تو مسئلہ بالکلیہ ہمارے موافق ہو گیا۔"),
            (2, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ مخالف کے اس استشہاد کا جواب دیتے ہیں جو ا"),
            (2, "end", " سے عام نہیں مانا، پس معلوم ہوا کہ ان تعریفوں میں «من» کا سہارا لینا بھی غلط ہے۔"),
            (3, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ مخالف کے اس استدلال کا جواب دیتے ہیں جو ا"),
            (3, "end", "نم کی دو قسمیں ہیں مالہ صوف و مالہ شعر، تو لفظ مساوی مان کر بھی حصہ کامل ہو گیا۔"),
            (4, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ ضان کی تعریف «مالہ صوف» میں"),
            (4, "end", "انور بھی صوف والا ہے، لہذا اب بات واضح طور پر ثابت ہو گئی کہ بھیڑ بھی ضان ہی ہے۔"),
            (5, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ میں نے پہلے کہا تھا کہ ہو س"),
            (5, "end", "ے معنی سے وہم کیا ہے — وہم کا مطلب ہے غلط خیال — اور اس کی حقیقت پر غور باقی ہے۔"),
            (6, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ یہ لطیفہ بھی قابل ملاحظہ ہے"),
            (6, "end", "ھڑ گئی جس کی کوئی جڑ نہیں، والحمد للہ علی توالی آلائہ کقطر المطر و امواج البحار۔"),
            (7, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ میری مانو تو میں تم کو نور "),
            (7, "end", "مجھ لو کہ میں نے تمہارے سامنے حال کی حقیقت بغیر کسی شک کے کھول کر بیان کر دی ہے۔"),
            (8, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ اب تھوڑی دیر چکتی پر بحث ہو"),
            (8, "end", "اہر ہو، کہ جو سورج کو طلوع ہوتے نہ دیکھے اس پر لازم ہے کہ اہل نظر کی بات مان لے۔"),
            (9, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ گزشتہ تحریروں سے یہ واضح ہو"),
            (9, "end", "سب دماغی خدشات اور وہمی خیالات ہیں، اسی طرح تحقیق کرنی چاہیے، واللہ ولی التوفیق۔"),
            (10, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ یہ بھی واضح ہوا کہ بھیڑ کی "),
            (10, "end", "ل ہے جو ائمہ اکامل کی نصوص کے خلاف ہے — نصوص کا مطلب ہے صاف صاف لکھی ہوئی باتیں۔"),
            (11, "start", "اس تنبیہ میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ اور اگر ہم سب چھوڑ چھا کر ی"),
            (11, "end", "ا ہوا — والحمد للہ الاحد الصمد، اس پر کہ اس نے ہم پر بے شمار نعمتیں عطا فرمائیں۔"),
            (12, "start", "اس تذییل میں اعلیٰ حضرت رحمۃ اللہ علیہ فرماتے ہیں کہ آپ کے ساتھ مستند کتابوں میں"),
        ],
    )
    join_cross_id(
        "jild20-masala-293",
        "jild20-masala-293",
        ["jild20-safia-tail"],
        "head_only",
        [
            (0, "end", "وشت کھائے اور مالدار اور فقیر جس کو چاہے کھلائے اور صدقہ تہائی حصہ سے کم نہ کرے،"),
            (1, "start", "یہ رسالے کا آخری حصہ ہے۔ اس میں اعلیٰ حضرت رحمۃ اللہ علیہ اس اعتراض کا جواب مکمل"),
        ],
    )
    assert len(all_items) == len({it["id"] for it in all_items}), (
        f"DUPLICATE IDS after joins: "
        f"{[i for i in [it['id'] for it in all_items] if [it['id'] for it in all_items].count(i) > 1]}"
    )

    print(f"after joins: {len(all_items)} items (272 concatenated)")
    # NO renames at merge: the 5 print-error renames were already
    # applied in the chunk files; assert them present.
    assert "jild20-masala-248" in [it["id"] for it in all_items], f"rename jild20-masala-248 missing from merged items"
    assert "jild20-masala-184" in [it["id"] for it in all_items], f"rename jild20-masala-184 missing from merged items"
    assert "jild20-masala-235" in [it["id"] for it in all_items], f"rename jild20-masala-235 missing from merged items"
    assert "jild20-masala-237" in [it["id"] for it in all_items], f"rename jild20-masala-237 missing from merged items"
    assert "jild20-p442" in [it["id"] for it in all_items], f"rename jild20-p442 missing from merged items"
    print("renames applied: [] (5 print-error renames already in the leaves)")
    # STABLE SORT by start_page, mirroring the v3 merge.
    all_items.sort(key=lambda it: it["start_page"])
    print(f"sorted by start_page: {len(all_items)} items")
    print(f"start_page derived for {len(derived)} items (expected 0: all chunks carry start_page)")
    assert len(all_items) == EXPECTED_ITEMS, (
        f"expected {EXPECTED_ITEMS} items, got {len(all_items)}"
    )

    # Rebuild each item in the canonical key order (the JSON contract).
    canonical = []
    for it in all_items:
        assert set(it.keys()) == set(ITEM_KEYS), (
            f"{it.get('id')}: keys {sorted(it.keys())} != {sorted(ITEM_KEYS)}"
        )
        canonical.append({k: it[k] for k in ITEM_KEYS})
    all_items = canonical

    # Validation.
    ids = [it["id"] for it in all_items]
    assert ids == V3_IDS, (
        f"ID sequence != v3: first diff at "
        f"{next((i for i, (a, b) in enumerate(zip(ids, V3_IDS)) if a != b), min(len(ids), len(V3_IDS)))}"
    )
    assert len(ids) == len(set(ids)), (
        f"DUPLICATE IDS: {[i for i in ids if ids.count(i) > 1]}"
    )
    empty = [it["id"] for it in all_items if not it.get("tashreeh_jawab")]
    assert not empty, f"EMPTY JAWAB: {empty}"
    vols = set(it.get("volume_id") for it in all_items)
    assert vols == EXPECTED_VOLUME_IDS, f"volume_ids {vols} != {EXPECTED_VOLUME_IDS}"
    pages = set()
    for it in all_items:
        assert it["pages"] == V3_PAGES[it["id"]], (
            f"{it['id']}: pages {it['pages']} != v3 {V3_PAGES[it['id']]}"
        )
        pages.update(it["pages"])
    assert len(pages) == EXPECTED_PAGES, (
        f"expected {EXPECTED_PAGES} unique pages, got {len(pages)}"
    )
    assert min(pages) == EXPECTED_MIN_PAGE, f"min page {min(pages)}"
    assert max(pages) == EXPECTED_MAX_PAGE, f"max page {max(pages)}"
    exceptions = {
        it["id"] for it in all_items if it["start_page"] != min(it["pages"])
    }
    assert not exceptions, f"start_page != min(pages): {sorted(exceptions)}"
    assert ids[0] == EXPECTED_FIRST_ID, f"first id {ids[0]}"
    assert ids[-1] == EXPECTED_LAST_ID, f"last id {ids[-1]}"

    out = {"masail": all_items}
    out_text = json.dumps(out, ensure_ascii=False, indent=1)
    assert LENTICULAR_PREFIX not in out_text, "lenticular markers remain in output!"
    assert not SHORT_PREFIX_RE.search(out_text), "short markers remain in output!"
    total_markers = len(RE_FULL.findall(out_text))
    print(
        f"VALIDATION PASSED: {len(all_items)} items, {len(pages)} unique pages "
        f"(min {min(pages)}, max {max(pages)}), volume_ids {sorted(vols)}, "
        f"first {ids[0]}, last {ids[-1]}, start_page == min(pages) for all, "
        f"derived start_page for {len(derived)} items, markers {total_markers}"
    )

    with open(os.path.join(BASE, "jild-20.json"), "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"WROTE jild-20.json: {len(out_text.encode('utf-8'))} bytes")


if __name__ == "__main__":
    main()
