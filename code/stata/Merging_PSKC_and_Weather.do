****************************************************
*
* Merge KCPS (PSKC) long file with daily sunshine exposures
*
****************************************************
clear all
set more off

****************************************************
* PATHS
****************************************************
global PROJ      "C:\Users\jaspe\OneDrive\Desktop\Research\Projects\Sunlight_ChildObesity"
global DERIVED   "$PROJ\derived"
global PROCESSED "$PROJ\processed"

* Inputs
global CENTERS   "$DERIVED\sigungu2010_centers_UTMK.csv"
global SUN_DAILY "$DERIVED\sigungu_daily_sunlight_20070601_20110831.dta"
global KCPS_IN   "$DERIVED\KCPS_long_w1to7_nomove_until_w3_dropfoster.dta"

* Output (FINAL)
global KCPS_OUT  "$PROCESSED\analysis_ready_PSKC.dta"

* Assumed DOB day within month (baseline = 15)
local DOBDAY = 15


****************************************************
* 0) Save FULL KCPS data first (so we never lose variables)
****************************************************
use "$KCPS_IN", clear
tempfile KCPS_FULL
save `KCPS_FULL', replace


****************************************************
* 1) Build mapping: resid_area -> SIGUNGU_CD  (from centers CSV)
****************************************************
import delimited using "$CENTERS", clear encoding("utf-8") varnames(1)

* --- Auto-detect SIGUNGU code column and rename to SIGUNGU_CD ---
ds
local codevar ""
foreach v of varlist _all {
    if strpos(lower("`v'"), "sigungu") & strpos(lower("`v'"), "cd") {
        local codevar "`v'"
        continue, break
    }
}
if "`codevar'"=="" {
    di as error "Could not find a SIGUNGU code column in centers CSV. Run: describe"
    error 111
}

rename `codevar' SIGUNGU_CD

* resid_area should exist; if not, try to detect
capture confirm variable resid_area
if _rc {
    local areavar ""
    foreach v of varlist _all {
        if strpos(lower("`v'"), "resid") | strpos(lower("`v'"), "area") {
            local areavar "`v'"
            continue, break
        }
    }
    if "`areavar'"=="" {
        di as error "Could not find resid_area column in centers CSV. Run: describe"
        error 111
    }
    rename `areavar' resid_area
}

keep SIGUNGU_CD resid_area
replace resid_area = ustrregexra(resid_area, "\s+", "")

* Make SIGUNGU_CD numeric (merge-safe)
destring SIGUNGU_CD, replace force

tempfile MAP
save `MAP', replace


****************************************************
* 2) Prepare sunshine cumulative sums by SIGUNGU_CD
****************************************************
use "$SUN_DAILY", clear

* Make SIGUNGU_CD numeric
destring SIGUNGU_CD, replace force

* date is typically "YYYY-MM-DD" string
capture confirm numeric variable date
if _rc {
    gen d = daily(date, "YMD")
    format d %td
    drop date
    rename d date
}

sort SIGUNGU_CD date
by SIGUNGU_CD: gen cum_rep  = sum(sun_hr_rep)
by SIGUNGU_CD: gen cum_cent = sum(sun_hr_centroid)

* Keep only what we need
keep SIGUNGU_CD date cum_rep cum_cent
tempfile SUN_CUM
save `SUN_CUM', replace

* Store min/max dates (for optional clipping)
summ date, meanonly
local SUN_MIN = r(min)
local SUN_MAX = r(max)


****************************************************
* 3) Build child-level exposure dataset (one row per N_ID)
*    NOTE: This step intentionally keeps only needed variables,
*          but FULL KCPS is already saved in `KCPS_FULL`.
****************************************************
use `KCPS_FULL', clear

* Use waves 1-4 to define baseline residence & birth month
keep if inlist(wave,1,2,3,4)

* Clean resid_area for mapping
replace resid_area = ustrregexra(resid_area, "\s+", "")

* Baseline residence: earliest wave for each child
sort N_ID wave
by N_ID: keep if _n==1

* Birth month: first nonmissing among wave-specific vars you listed
gen bmonth = BCh08dmg006a
replace bmonth = DCh09dmg006a if missing(bmonth)
replace bmonth = DCh10dmg006a if missing(bmonth)
replace bmonth = DCh11dmg006a if missing(bmonth)
replace bmonth = DCh12dmg006a if missing(bmonth)
replace bmonth = DCh13dmg006a if missing(bmonth)
replace bmonth = DCh14dmg006a if missing(bmonth)

* PSKC cohort birth year = 2008
gen byear = 2008

* Merge SIGUNGU_CD based on baseline resid_area
merge m:1 resid_area using `MAP', keep(match master) nogen

* Ensure numeric merge key
destring SIGUNGU_CD, replace force

* Impute DOB = 15th of birth month
gen dob = mdy(bmonth, `DOBDAY', byear)
format dob %td

* Month index for shifting
gen dob_m = mofd(dob)
format dob_m %tm

****************************************************
* 4) Define daily window boundaries (anchored at DOBDAY)
****************************************************
* Prenatal trimesters: [-9,-6), [-6,-3), [-3,0)
gen pre1_s = dofm(dob_m-9) + (`DOBDAY'-1)
gen pre1_e = dofm(dob_m-6) + (`DOBDAY'-1) - 1

gen pre2_s = dofm(dob_m-6) + (`DOBDAY'-1)
gen pre2_e = dofm(dob_m-3) + (`DOBDAY'-1) - 1

gen pre3_s = dofm(dob_m-3) + (`DOBDAY'-1)
gen pre3_e = dob - 1

* Postnatal: [0,6), [6,12), [12,24), [24,36)
gen m0_6_s   = dob
gen m0_6_e   = dofm(dob_m+6)  + (`DOBDAY'-1) - 1

gen m6_12_s  = dofm(dob_m+6)  + (`DOBDAY'-1)
gen m6_12_e  = dofm(dob_m+12) + (`DOBDAY'-1) - 1

gen m12_24_s = dofm(dob_m+12) + (`DOBDAY'-1)
gen m12_24_e = dofm(dob_m+24) + (`DOBDAY'-1) - 1

gen m24_36_s = dofm(dob_m+24) + (`DOBDAY'-1)
gen m24_36_e = dofm(dob_m+36) + (`DOBDAY'-1) - 1

* Optional: clip to sunshine availability (safe-guard)
foreach v in pre1_s pre1_e pre2_s pre2_e pre3_s pre3_e ///
            m0_6_s m0_6_e m6_12_s m6_12_e m12_24_s m12_24_e m24_36_s m24_36_e {
    replace `v' = `SUN_MIN' if `v' < `SUN_MIN' & !missing(`v')
    replace `v' = `SUN_MAX' if `v' > `SUN_MAX' & !missing(`v')
}

format pre1_s pre1_e pre2_s pre2_e pre3_s pre3_e ///
       m0_6_s m0_6_e m6_12_s m6_12_e m12_24_s m12_24_e m24_36_s m24_36_e %td

keep N_ID SIGUNGU_CD resid_area dob ///
     pre1_s pre1_e pre2_s pre2_e pre3_s pre3_e ///
     m0_6_s m0_6_e m6_12_s m6_12_e m12_24_s m12_24_e m24_36_s m24_36_e


****************************************************
* 5) Create child-window long dataset (7 windows)
****************************************************
tempfile CHILD_BASE WLONG

* At this point you should be in the child-level dataset that has:
* N_ID SIGUNGU_CD pre1_s pre1_e ... m24_36_s m24_36_e
save `CHILD_BASE', replace

clear
save `WLONG', emptyok

foreach W in pre1 pre2 pre3 m0_6 m6_12 m12_24 m24_36 {
    use `CHILD_BASE', clear
    gen window = "`W'"
    gen start  = `W'_s
    gen end    = `W'_e
    keep N_ID SIGUNGU_CD window start end
    append using `WLONG'
    save `WLONG', replace
}

use `WLONG', clear

tab window
summ start end

gen startm1 = start - 1
format start end startm1 %td


****************************************************
* 6) Interval sums via cumulative-sum subtraction
****************************************************
* Merge cum at end
rename end date
merge m:1 SIGUNGU_CD date using `SUN_CUM', keep(match master) nogen
rename cum_rep  cum_end_rep
rename cum_cent cum_end_cent
rename date end

* Merge cum at start-1
rename startm1 date
merge m:1 SIGUNGU_CD date using `SUN_CUM', keep(master match) nogen
rename cum_rep  cum_start_rep
rename cum_cent cum_start_cent
rename date startm1

replace cum_start_rep  = 0 if missing(cum_start_rep)
replace cum_start_cent = 0 if missing(cum_start_cent)

gen exp_rep  = cum_end_rep  - cum_start_rep
gen exp_cent = cum_end_cent - cum_start_cent

keep N_ID window exp_rep exp_cent

reshape wide exp_rep exp_cent, i(N_ID) j(window) string

tempfile EXPOSURES
save `EXPOSURES', replace


****************************************************
* 7) Reload FULL KCPS and merge exposures back (KEEP ALL VARS)
****************************************************
use `KCPS_FULL', clear

* Merge one row per child exposures onto long KCPS (many rows per N_ID)
merge m:1 N_ID using `EXPOSURES', keep(master match) nogen


****************************************************
* 8) Generate obesity related variables (using KDCA cutoffs)
****************************************************

* -------------------------------
* 8.1) Generate unified weight and height variables by wave
* -------------------------------
gen weight_kg = .
gen height_cm = .

replace weight_kg = ECh09hlt012 if wave==2
replace height_cm = ECh09hlt013 if wave==2

replace weight_kg = ECh10hlt012 if wave==3
replace height_cm = ECh10hlt013 if wave==3

replace weight_kg = DCh11hlt012 if wave==4
replace height_cm = DCh11hlt013 if wave==4

replace weight_kg = DCh12hlt012 if wave==5
replace height_cm = DCh12hlt013 if wave==5

replace weight_kg = JCh13hlt012 if wave==6
replace height_cm = JCh13hlt013 if wave==6

replace weight_kg = JCh14hlt012 if wave==7
replace height_cm = JCh14hlt013 if wave==7

* [NOTE] Unobserved values that were previously 99999999 are converted to 1.00e+08. We will treat these values "missing."

replace weight_kg = . if weight_kg >= 99900000
replace height_cm = . if height_cm >= 99900000

* -------------------------------
* 8.2) Define BMI
* -------------------------------
gen height_m = height_cm/100
replace height_m = . if missing(height_m)
gen bmi = weight_kg/(height_m^2) if !missing(weight_kg, height_m)
label var bmi "BMI (kg/m^2)"

* -------------------------------
* 8.3) Create sex variable
* -------------------------------
egen sex = rowfirst(BCh08dmg001 DCh09dmg001 DCh10dmg001 DCh11dmg001 DCh12dmg001 DCh13dmg001 DCh14dmg001)
bys N_ID: replace sex = sex[1]
label define sexlbl 1 "male" 2 "female"
label values sex sexlbl

* -------------------------------
* 8.4) Create age-in-month variable
* -------------------------------
gen age_m = .
replace age_m = DCh09dmg006 if wave==2
replace age_m = DCh10dmg006 if wave==3
replace age_m = DCh11dmg006 if wave==4
replace age_m = DCh12dmg006 if wave==5
replace age_m = DCh13dmg006 if wave==6
replace age_m = DCh14dmg006 if wave==7

* Merge key for KDCA: round down months
gen agemos = floor(age_m)
label var agemos "Age in months (floor) for KDCA merge"

* -------------------------------
* 8.5) Import KDCA BMI cutoffs and merge
*     Sheet: 연령별 체질량지수
*     A=sex(1: male/2: female), C=age in months, O=overweight cutoff, Q=obesity cutoff
*
* The reference file can be accessed at "https://knhanes.kdca.go.kr/knhanes/grtcht/dwnld/dtLst.do"
* -------------------------------
local KDCA_XLS "C:\Users\jaspe\OneDrive\Desktop\Data\Child_Growth\Child_Growth.xls"

preserve
    import excel using "`KDCA_XLS'", ///
        sheet("연령별 체질량지수") cellrange(A3:S410) clear

    * Stata names columns A B C ... Q R S
    rename A sex
    rename C agemos
    rename O bmi_cut_overwt
    rename Q bmi_cut_obese

    keep sex agemos bmi_cut_overwt bmi_cut_obese
    destring sex agemos bmi_cut_overwt bmi_cut_obese, replace force
    drop if missing(sex) | missing(agemos)

    isid sex agemos

    tempfile kdca
    save `kdca', replace
restore

merge m:1 sex agemos using `kdca', keep(master match) nogen

* -------------------------------
* 8.6) Define overweight/obesity using KDCA cutoffs
* -------------------------------
gen overweight_kdca = (bmi >= bmi_cut_overwt & bmi < bmi_cut_obese) if !missing(bmi, bmi_cut_overwt, bmi_cut_obese)
gen obese_kdca      = (bmi >= bmi_cut_obese)                        if !missing(bmi, bmi_cut_obese)

label var overweight_kdca "Overweight (KDCA BMI cutoff)"
label var obese_kdca      "Obesity (KDCA BMI cutoff)"


****************************************************
* 9) Save the outcome
****************************************************

* Save to PROCESSED (absolute path)
save "$KCPS_OUT", replace
display as text "Saved final dataset to: $KCPS_OUT"
