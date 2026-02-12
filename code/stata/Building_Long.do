/*************************************************************************************************************

This do-file processes datasets required for the baseline analysis in the following order:
Build KCPS long panel (waves 1-7), keep non-movers through wave 3, and drop foster children (waves 1-4)

Inputs: w1(2008).dta ... w7(2014).dta	(in ${DATA_ROOT}\PSKC\Data)
Output:  KCPS_long_w1to7_nomove_until_w3_dropfoster.dta (in ${DATA_ROOT}\derived)

*************************************************************************************************************/

version 19.5
clear all
set maxvar 120000

*-----------------------------*
* 0) Paths 				      
*-----------------------------*

global DATA_ROOT "C:\Users\jaspe\OneDrive\Desktop\Data"
global PROJ_ROOT "C:\Users\jaspe\OneDrive\Desktop\Research\Projects\Sunlight_ChildObesity"

global raw     "${DATA_ROOT}\PSKC\Data"
global weather "${DATA_ROOT}\Weather"

global do      "${PROJ_ROOT}\do"
global derived "${PROJ_ROOT}\derived"
global output  "${PROJ_ROOT}\output"
global logs    "${PROJ_ROOT}\logs"

cap mkdir "${derived}"
cap mkdir "${output}"
cap mkdir "${logs}"

* Logging

cap log close _all
log using "${logs}\build.log", replace text


*--------------------------------------------------------*
* 1) Files and the district variable names               
*--------------------------------------------------------*
local files w1(2008).dta w2(2009).dta w3(2010).dta w4(2011).dta w5(2012).dta w6(2013).dta w7(2014).dta
local resid_dist DHu08cmm015 DHu09cmm015 DHu10cmm015 DHu11cmm015 EHu12cmm015 DHu13cmm015 DHu14cmm015

* Build empty master
tempfile master
save `master', emptyok replace

local W = 0
foreach f of local files {
    local ++W

    * Load wave file
    use "${raw}/`f'", clear
    gen wave = `W'

    * Pick the district variable for this wave
    local dv : word `W' of `resid_dist'
    capture confirm variable `dv'
    if _rc {
        di as error "District var `dv' not found in `f'."
        exit 198
    }

    * Standardize to resid_area
    rename `dv' resid_area

    * Ensure string
    capture confirm string variable resid_area
	if _rc {
    tostring resid_area, replace format(%20.0f)
    replace resid_area = "" if regexm(strtrim(resid_area), "^\.[a-z]?$")
	}
	replace resid_area = strtrim(resid_area)


    append using `master'
    save `master', replace
}

use `master', clear

*-----------------------------------------------------------------*
* 3) Keep IDs observed in waves 1-3 AND non-movers through wave 3
*-----------------------------------------------------------------*
preserve
    keep if inrange(wave, 1, 3)
    keep N_ID wave resid_area

    * Check unique (N_ID, wave)
    cap noisily isid N_ID wave
    if _rc {
        disp as error "Not unique on N_ID wave in waves 1–3. Check for duplicates before reshape."
        duplicates report N_ID wave
        exit 459
    }

    reshape wide resid_area, i(N_ID) j(wave)

    * Require all three waves have a non-missing district
    drop if resid_area1=="" | resid_area2=="" | resid_area3==""

    * Keep only non-movers through wave 3 (w1=w2=w3)
    keep if resid_area1 == resid_area2 & resid_area1 == resid_area3

    keep N_ID
    tempfile keepIDs
    save `keepIDs', replace
restore

* Keep only those non-mover IDs in the full long dataset
merge m:1 N_ID using `keepIDs'
tab _merge
keep if _merge == 3
drop _merge

*--------------------------------------*
* 4) Drop foster children (waves 1–4)
*--------------------------------------*
gen foster = ///
    (wave==1 & DHu08dmg010==2) | ///
    (wave==2 & DHu09dmg010==2) | ///
    (wave==3 & DHu10dmg010==2) | ///
    (wave==4 & DHu11dmg010==2)

bysort N_ID: egen drop_id = max(foster)
drop if drop_id == 1
drop foster drop_id


*-------------------------------------------------------------*
* 4.5) Drop ever-non-participants in any wave (any "미참여")
*-------------------------------------------------------------*

* Collapse wave-specific non-participation var into one per-row indicator
gen byte nonresp = .

replace nonresp = (DHu08int001==9) if wave==1
replace nonresp = (DHu09int001==9) if wave==2
replace nonresp = (DHu10int001==9) if wave==3
replace nonresp = (DHu11int001==9) if wave==4
replace nonresp = (DHu12int001==9) if wave==5
replace nonresp = (DHu13int001==9) if wave==6
replace nonresp = (DHu14int001==9) if wave==7

* Sanity check: int001 should not be missing
count if missing(nonresp)
if r(N) > 0 {
    di as error "ERROR: nonresp is missing for " r(N) " observations. Check wave-specific nonresp vars / wave coding."
    list N_ID wave resid_area if missing(nonresp), sepby(N_ID) noobs in 1/50
    exit 459
}

* Drop ever-attriters
bys N_ID: egen byte ever_attrit = max(nonresp)
drop if ever_attrit==1

drop nonresp ever_attrit


*----------------------------------------------*
* 5) Final checks + save to "derived" folder
*----------------------------------------------*
cap noisily isid N_ID wave

save "${derived}\PSKC_long_w1to7_nomove_until_w3_dropfoster.dta", replace
log close
