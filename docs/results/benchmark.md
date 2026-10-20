# Card benchmark (2026-10-19 15:55 UTC, quiesced, median of 5 runs after 1 warm-up)

| Card | core (PostgreSQL) ms | optimised (ClickHouse) ms | speed-up | rows core / opt | result |
|---|---:|---:|---:|---:|---|
| 1. Daily successful transactions (90 days) | 2666 | 45 | x59 | 90 / 90 | identical |
| 2. Daily success rate by channel (30 days) | 1197 | 45 | x27 | 90 / 90 | identical |
| 3. Top 10 merchants this month | 409 | 55 | x7 | 10 / 10 | identical |
| 4. Fee by MCC and province (90 days) | 1818 | 91 | x20 | 308 / 308 | differs - see comparison |
| 5. Monthly successful volume by risk level | 6019 | 109 | x55 | 21 / 21 | differs - see comparison |

## Comparison details

```json
{
  "1": {
    "identical": true,
    "differences": []
  },
  "2": {
    "identical": true,
    "differences": []
  },
  "3": {
    "identical": true,
    "differences": []
  },
  "4": {
    "identical": false,
    "differences": [
      "row 50: core=('5411', 'Markazi', 15118052) optimised=('5411', 'Markazi', 16534201)",
      "row 51: core=('5411', 'Mazandaran', 21996979) optimised=('5411', 'Mazandaran', 20580830)",
      "row 70: core=('5541', 'Hamadan', 18006916) optimised=('5541', 'Hamadan', 16364212)",
      "row 75: core=('5541', 'Khorasan Razavi', 25748659) optimised=('5541', 'Khorasan Razavi', 27466500)",
      "row 85: core=('5541', 'Semnan', 7241599) optimised=('5541', 'Semnan', 5523758)",
      "row 86: core=('5541', 'Sistan and Baluchestan', 13341555) optimised=('5541', 'Sistan and Baluchestan', 14984259)",
      "row 111: core=('5691', 'Markazi', 6718177) optimised=('5691', 'Markazi', 12879299)",
      "row 121: core=('5691', 'Yazd', 30821437) optimised=('5691', 'Yazd', 24660315)",
      "row 133: core=('5732', 'Ilam', 13029033) optimised=('5732', 'Ilam', 15485407)",
      "row 142: core=('5732', 'Markazi', 26361009) optimised=('5732', 'Markazi', 23627632)",
      "row 150: core=('5732', 'Tehran', 87826531) optimised=('5732', 'Tehran', 90559908)",
      "row 151: core=('5732', 'West Azerbaijan', 18710201) optimised=('5732', 'West Azerbaijan', 16253827)",
      "row 155: core=('5812', 'Ardabil', 13502169) optimised=('5812', 'Ardabil', 14882670)",
      "row 159: core=('5812', 'Fars', 18111869) optimised=('5812', 'Fars', 14991512)",
      "row 160: core=('5812', 'Gilan', 11124999) optimised=('5812', 'Gilan', 14245356)",
      "row 162: core=('5812', 'Hamadan', 15054636) optimised=('5812', 'Hamadan', 13674135)",
      "row 198: core=('5912', 'Kermanshah', 21670330) optimised=('5912', 'Kermanshah', 35512895)",
      "row 208: core=('5912', 'Qom', 29185388) optimised=('5912', 'Qom', 15342823)",
      "row 220: core=('5999', 'East Azerbaijan', 10364745) optimised=('5999', 'East Azerbaijan', 8814864)",
      "row 226: core=('5999', 'Ilam', 14166390) optimised=('5999', 'Ilam', 15716271)"
    ]
  },
  "5": {
    "identical": false,
    "differences": [
      "row 0: core=('2026-04-01', 'HIGH', 22560, 24522144000) optimised=('2026-04-01', 'HIGH', 16489, 17954841000)",
      "row 1: core=('2026-04-01', 'LOW', 119619, 130736093000) optimised=('2026-04-01', 'LOW', 120608, 131696533000)",
      "row 2: core=('2026-04-01', 'MEDIUM', 39977, 42989289000) optimised=('2026-04-01', 'MEDIUM', 45059, 48596152000)",
      "row 3: core=('2026-05-01', 'HIGH', 85949, 93905920000) optimised=('2026-05-01', 'HIGH', 62582, 68678317000)",
      "row 4: core=('2026-05-01', 'LOW', 451521, 494294423000) optimised=('2026-05-01', 'LOW', 455246, 498264238000)",
      "row 5: core=('2026-05-01', 'MEDIUM', 151539, 166555840000) optimised=('2026-05-01', 'MEDIUM', 171181, 187813628000)",
      "row 6: core=('2026-06-01', 'HIGH', 83162, 90578265000) optimised=('2026-06-01', 'HIGH', 60463, 65716895000)",
      "row 7: core=('2026-06-01', 'LOW', 436796, 475637986000) optimised=('2026-06-01', 'LOW', 440426, 479828909000)",
      "row 8: core=('2026-06-01', 'MEDIUM', 146665, 161209919000) optimised=('2026-06-01', 'MEDIUM', 165734, 181880366000)",
      "row 9: core=('2026-07-01', 'HIGH', 85540, 94079167000) optimised=('2026-07-01', 'HIGH', 62572, 69305557000)",
      "row 10: core=('2026-07-01', 'LOW', 450970, 492953047000) optimised=('2026-07-01', 'LOW', 454789, 497050470000)",
      "row 11: core=('2026-07-01', 'MEDIUM', 152095, 166027156000) optimised=('2026-07-01', 'MEDIUM', 171244, 186703343000)",
      "row 12: core=('2026-08-01', 'HIGH', 86095, 94891378000) optimised=('2026-08-01', 'HIGH', 62765, 69021042000)",
      "row 13: core=('2026-08-01', 'LOW', 451291, 491079002000) optimised=('2026-08-01', 'LOW', 455088, 495265331000)",
      "row 14: core=('2026-08-01', 'MEDIUM', 151578, 165682820000) optimised=('2026-08-01', 'MEDIUM', 171111, 187366827000)",
      "row 15: core=('2026-09-01', 'HIGH', 82789, 90381537000) optimised=('2026-09-01', 'HIGH', 60297, 65874158000)",
      "row 16: core=('2026-09-01', 'LOW', 437027, 478756330000) optimised=('2026-09-01', 'LOW', 440743, 482794926000)",
      "row 17: core=('2026-09-01', 'MEDIUM', 147024, 160961403000) optimised=('2026-09-01', 'MEDIUM', 165800, 181430186000)",
      "row 18: core=('2026-10-01', 'HIGH', 53303, 57768435000) optimised=('2026-10-01', 'HIGH', 38915, 42013306000)",
      "row 19: core=('2026-10-01', 'LOW', 283060, 308874639000) optimised=('2026-10-01', 'LOW', 285471, 311506718000)"
    ]
  },
  "4_current_attributes": {
    "identical": true,
    "note": "card 4 recomputed on the fact table with CURRENT merchant attributes"
  },
  "merchant_changes_captured": 14
}
```