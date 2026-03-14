
I have figure out a way to call API to download/fetch GST data using API. Following are the details of fetching GST data for UClean uc_orders_sync

Request URL: https://store.ucleanlaundry.com/api/v1/stores/generateGST?franchise=UCLEAN
Request Method POST
Request Paylod:
{from_date: "2026-02-01", to_date: "2026-02-15"}

Request Headers
accept
application/json, text/plain, */*
authorization
Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NTQ2LCJpYXQiOjE3NzExNTgzMjUsImV4cCI6MTc3Mzc1MDMyNX0.-i1qfOc6pevoK6P1B2jTCJsoSN_Pd2i2Wg9SunNWc2o
content-type
application/json
referer
https://store.ucleanlaundry.com/gst-report
sec-ch-ua
"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"
sec-ch-ua-mobile
?0
sec-ch-ua-platform
"macOS"
user-agent
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36

Response:
{
  "data": [
    {
      "order_number": "UC610-0891",
      "invoice_number": "UC610-2025-26-768",
      "invoice_date": "2026-02-01 12:11:44",
      "name": "RAJESHKAR",
      "customer_phone": "8870700102",
      "customer_gst": null,
      "address": "house no. 445, , hotel, hotel, sector 27",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 633.15,
      "cgst": 56.99,
      "sgst": 56.99,
      "total_tax": 113.97,
      "final_amount": 747.12,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0892",
      "invoice_number": "UC610-2025-26-769",
      "invoice_date": "2026-02-01 13:05:57",
      "name": "Neha Kumari ",
      "customer_phone": "9637130417",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 755.27,
      "cgst": 67.97,
      "sgst": 67.97,
      "total_tax": 135.95,
      "final_amount": 891.22,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0894",
      "invoice_number": "UC610-2025-26-770",
      "invoice_date": "2026-02-01 13:31:07",
      "name": "MRIDUL",
      "customer_phone": "7407653122",
      "customer_gst": null,
      "address": "SEC 43 SEC 43, , SHUSANT LOK, SHUSANT LOK, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 538.2,
      "cgst": 48.44,
      "sgst": 48.44,
      "total_tax": 96.88,
      "final_amount": 635.08,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0895",
      "invoice_number": "UC610-2025-26-771",
      "invoice_date": "2026-02-01 18:29:50",
      "name": "VAIBHAV GOEL",
      "customer_phone": "7082295934",
      "customer_gst": null,
      "address": "SECTOR 30, , SEC 30, SEC 30, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 592.84,
      "cgst": 53.35,
      "sgst": 53.35,
      "total_tax": 106.71,
      "final_amount": 699.55,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0896",
      "invoice_number": "UC610-2025-26-772",
      "invoice_date": "2026-02-01 19:22:10",
      "name": "Alok Raj",
      "customer_phone": "9708336185",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 2988,
      "cgst": 268.92,
      "sgst": 268.92,
      "total_tax": 537.84,
      "final_amount": 3525.84,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0899",
      "invoice_number": "UC610-2025-26-773",
      "invoice_date": "2026-02-02 16:02:38",
      "name": "Sameer Singal",
      "customer_phone": "9996712662",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 707.51,
      "cgst": 63.67,
      "sgst": 63.67,
      "total_tax": 127.35,
      "final_amount": 834.86,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0900",
      "invoice_number": "UC610-2025-26-774",
      "invoice_date": "2026-02-02 17:47:27",
      "name": "Abhi ",
      "customer_phone": "9530823023",
      "customer_gst": null,
      "address": "29428 Sec 43, , Sushant lok 1, Sushant lok 1, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1336.5,
      "cgst": 120.29,
      "sgst": 120.29,
      "total_tax": 240.57,
      "final_amount": 1577.07,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0901",
      "invoice_number": "UC610-2025-26-775",
      "invoice_date": "2026-02-03 11:24:12",
      "name": " Kamya Mehra",
      "customer_phone": "917508420",
      "customer_gst": null,
      "address": "lose to Cyber City, Q3/12 DLF Phase - II Walking to Sikanderpur Metro station, Mehrauli-Gurgaon Rd, , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 331.15,
      "cgst": 29.81,
      "sgst": 29.81,
      "total_tax": 59.61,
      "final_amount": 390.76,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0902",
      "invoice_number": "UC610-2025-26-776",
      "invoice_date": "2026-02-03 12:09:37",
      "name": "vaksi ",
      "customer_phone": "9266747250",
      "customer_gst": null,
      "address": "C1985 3RD FLOOR NEAR BY MEHNDI PARK , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 2947.49,
      "cgst": 265.27,
      "sgst": 265.27,
      "total_tax": 530.55,
      "final_amount": 3478.04,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0903",
      "invoice_number": "UC610-2025-26-777",
      "invoice_date": "2026-02-03 13:34:20",
      "name": "Mr. Sovik",
      "customer_phone": "8376014981",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1200.56,
      "cgst": 108.05,
      "sgst": 108.05,
      "total_tax": 216.1,
      "final_amount": 1416.66,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0905",
      "invoice_number": "UC610-2025-26-778",
      "invoice_date": "2026-02-03 19:11:57",
      "name": "kautuk ",
      "customer_phone": "9310675733",
      "customer_gst": null,
      "address": "F 4/10 - dlf phase 1 / second floor, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1247.69,
      "cgst": 112.29,
      "sgst": 112.29,
      "total_tax": 224.59,
      "final_amount": 1472.28,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0906",
      "invoice_number": "UC610-2025-26-779",
      "invoice_date": "2026-02-04 12:12:00",
      "name": "Pallavi",
      "customer_phone": "9717368885",
      "customer_gst": null,
      "address": "6838, 2nd floor green meadows, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 760,
      "cgst": 68.4,
      "sgst": 68.4,
      "total_tax": 136.8,
      "final_amount": 896.8,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0907",
      "invoice_number": "UC610-2025-26-780",
      "invoice_date": "2026-02-04 15:01:57",
      "name": "ritika ",
      "customer_phone": "9958773761",
      "customer_gst": null,
      "address": "C257 SUSHANT LOK 1 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 309.87,
      "cgst": 27.89,
      "sgst": 27.89,
      "total_tax": 55.78,
      "final_amount": 365.65,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0908",
      "invoice_number": "UC610-2025-26-781",
      "invoice_date": "2026-02-04 15:21:34",
      "name": "Galaxy Pizza ",
      "customer_phone": "9459756747",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 184,
      "cgst": 16.56,
      "sgst": 16.56,
      "total_tax": 33.12,
      "final_amount": 217.12,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0783",
      "invoice_number": "UC610-2025-26-782",
      "invoice_date": "2026-02-04 15:38:54",
      "name": "Combos Saloon ",
      "customer_phone": "7836087861",
      "customer_gst": null,
      "address": "Vyapar Kendra Sushant lok 1  GF shop 206 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 331.36,
      "cgst": 29.82,
      "sgst": 29.82,
      "total_tax": 59.64,
      "final_amount": 391,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0910",
      "invoice_number": "UC610-2025-26-783",
      "invoice_date": "2026-02-04 17:16:29",
      "name": "surya",
      "customer_phone": "8448271143",
      "customer_gst": null,
      "address": "SECTOR 53 DLF GURGAON , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 109,
      "cgst": 9.81,
      "sgst": 9.81,
      "total_tax": 19.62,
      "final_amount": 128.62,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0911",
      "invoice_number": "UC610-2025-26-784",
      "invoice_date": "2026-02-04 18:17:55",
      "name": "Abhinav Kanaujia ",
      "customer_phone": "7307781578",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 455.5,
      "cgst": 40.99,
      "sgst": 40.99,
      "total_tax": 81.99,
      "final_amount": 537.49,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0912",
      "invoice_number": "UC610-2025-26-785",
      "invoice_date": "2026-02-04 18:50:36",
      "name": "Rakshit",
      "customer_phone": "9952637837",
      "customer_gst": null,
      "address": "1138  Ground flor, , c block, c block, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 847.46,
      "cgst": 76.27,
      "sgst": 76.27,
      "total_tax": 152.54,
      "final_amount": 1000,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0915",
      "invoice_number": "UC610-2025-26-786",
      "invoice_date": "2026-02-05 11:44:39",
      "name": "vaksi ",
      "customer_phone": "9266747250",
      "customer_gst": null,
      "address": "C1985 3RD FLOOR NEAR BY MEHNDI PARK , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 614.28,
      "cgst": 55.28,
      "sgst": 55.28,
      "total_tax": 110.57,
      "final_amount": 724.85,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0916",
      "invoice_number": "UC610-2025-26-787",
      "invoice_date": "2026-02-05 15:45:46",
      "name": "Aru Srivastava ",
      "customer_phone": "8839648779",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 514.28,
      "cgst": 46.28,
      "sgst": 46.28,
      "total_tax": 92.57,
      "final_amount": 606.85,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0917",
      "invoice_number": "UC610-2025-26-788",
      "invoice_date": "2026-02-05 16:49:47",
      "name": "ravi",
      "customer_phone": "8860142914",
      "customer_gst": null,
      "address": "sec 43, , sushant lok, sushant lok, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 99,
      "cgst": 8.91,
      "sgst": 8.91,
      "total_tax": 17.82,
      "final_amount": 116.82,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0918",
      "invoice_number": "UC610-2025-26-789",
      "invoice_date": "2026-02-05 17:21:31",
      "name": "ritika ",
      "customer_phone": "9958773761",
      "customer_gst": null,
      "address": "C257 SUSHANT LOK 1 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 198.07,
      "cgst": 17.83,
      "sgst": 17.83,
      "total_tax": 35.65,
      "final_amount": 233.72,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0919",
      "invoice_number": "UC610-2025-26-790",
      "invoice_date": "2026-02-05 18:03:31",
      "name": "UDIT ",
      "customer_phone": "9899937799",
      "customer_gst": null,
      "address": "C2102 A BEHIND BEECH TREE BUILDING , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 432.25,
      "cgst": 38.9,
      "sgst": 38.9,
      "total_tax": 77.8,
      "final_amount": 510.05,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0913",
      "invoice_number": "UC610-2025-26-791",
      "invoice_date": "2026-02-05 18:37:52",
      "name": "MONIKA MAM",
      "customer_phone": "9873604296",
      "customer_gst": null,
      "address": "E Block - Richmond Park, F35Q+GFX, Sector 43 Service Road, DLF Garden Villas, DLF Phase IV, Sector 4, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 409.36,
      "cgst": 36.84,
      "sgst": 36.84,
      "total_tax": 73.68,
      "final_amount": 483.04,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0920",
      "invoice_number": "UC610-2025-26-792",
      "invoice_date": "2026-02-06 11:18:06",
      "name": "HARSHIT ",
      "customer_phone": "9953554480",
      "customer_gst": null,
      "address": "C777 FRESH ALLEY STORE C BLOCK , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 772.28,
      "cgst": 69.5,
      "sgst": 69.5,
      "total_tax": 139.01,
      "final_amount": 911.29,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0856",
      "invoice_number": "UC610-2025-26-793",
      "invoice_date": "2026-02-06 18:36:59",
      "name": "Puja Pandey",
      "customer_phone": "9821204814",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 947.65,
      "cgst": 85.29,
      "sgst": 85.29,
      "total_tax": 170.58,
      "final_amount": 1118.23,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0922",
      "invoice_number": "UC610-2025-26-794",
      "invoice_date": "2026-02-07 12:29:50",
      "name": "shashaank",
      "customer_phone": "9818934294",
      "customer_gst": null,
      "address": "C 844 A near vyapar, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1698.77,
      "cgst": 152.89,
      "sgst": 152.89,
      "total_tax": 305.78,
      "final_amount": 2004.55,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0923",
      "invoice_number": "UC610-2025-26-795",
      "invoice_date": "2026-02-07 12:42:13",
      "name": "Sohini Chowdhry",
      "customer_phone": "9681179496",
      "customer_gst": null,
      "address": "house no.2 2nd flor, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1014.2,
      "cgst": 91.28,
      "sgst": 91.28,
      "total_tax": 182.56,
      "final_amount": 1196.76,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0927",
      "invoice_number": "UC610-2025-26-796",
      "invoice_date": "2026-02-07 17:02:18",
      "name": "pratyush",
      "customer_phone": "9594183162",
      "customer_gst": null,
      "address": "C block 949, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 707,
      "cgst": 63.63,
      "sgst": 63.63,
      "total_tax": 127.26,
      "final_amount": 834.26,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0926",
      "invoice_number": "UC610-2025-26-797",
      "invoice_date": "2026-02-07 17:22:54",
      "name": "Ms. Jesley",
      "customer_phone": "9075226255",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 914.18,
      "cgst": 82.28,
      "sgst": 82.28,
      "total_tax": 164.55,
      "final_amount": 1078.73,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0928",
      "invoice_number": "UC610-2025-26-798",
      "invoice_date": "2026-02-07 17:50:47",
      "name": "Garvit Jain",
      "customer_phone": "9711439981",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 814.42,
      "cgst": 73.3,
      "sgst": 73.3,
      "total_tax": 146.6,
      "final_amount": 961.02,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0929",
      "invoice_number": "UC610-2025-26-799",
      "invoice_date": "2026-02-07 18:58:45",
      "name": "SAMAR KUMAR ",
      "customer_phone": "7700005018",
      "customer_gst": null,
      "address": "Stanza living, 152,  Silokra Rd, Vijay Vihar, Sector 30, Gurugram, , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 423.73,
      "cgst": 38.13,
      "sgst": 38.13,
      "total_tax": 76.27,
      "final_amount": 500,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0930",
      "invoice_number": "UC610-2025-26-800",
      "invoice_date": "2026-02-07 19:53:10",
      "name": "Subham",
      "customer_phone": "8861503553",
      "customer_gst": null,
      "address": "HOUSE NO 264 SECTOR 28 GURGAON , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 807.29,
      "cgst": 72.65,
      "sgst": 72.65,
      "total_tax": 145.31,
      "final_amount": 952.6,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0931",
      "invoice_number": "UC610-2025-26-801",
      "invoice_date": "2026-02-08 11:22:04",
      "name": "kritika ",
      "customer_phone": "7887639518",
      "customer_gst": null,
      "address": "sec 43, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 103,
      "cgst": 9.27,
      "sgst": 9.27,
      "total_tax": 18.54,
      "final_amount": 121.54,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0932",
      "invoice_number": "UC610-2025-26-802",
      "invoice_date": "2026-02-08 15:35:44",
      "name": "Vikash",
      "customer_phone": "9818837560",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 92.66,
      "cgst": 8.34,
      "sgst": 8.34,
      "total_tax": 16.68,
      "final_amount": 109.34,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0933",
      "invoice_number": "UC610-2025-26-803",
      "invoice_date": "2026-02-08 18:59:23",
      "name": "surya",
      "customer_phone": "8448271143",
      "customer_gst": null,
      "address": "SECTOR 53 DLF GURGAON , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 218,
      "cgst": 19.62,
      "sgst": 19.62,
      "total_tax": 39.24,
      "final_amount": 257.24,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0934",
      "invoice_number": "UC610-2025-26-804",
      "invoice_date": "2026-02-09 11:46:46",
      "name": " sabhya jain ",
      "customer_phone": "8650778120",
      "customer_gst": null,
      "address": " 1016, 1st floor sushant lok 1 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 323.1,
      "cgst": 29.08,
      "sgst": 29.08,
      "total_tax": 58.16,
      "final_amount": 381.26,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0898",
      "invoice_number": "UC610-2025-26-805",
      "invoice_date": "2026-02-09 11:54:00",
      "name": "Ranodeep",
      "customer_phone": "7688055417",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 726.3,
      "cgst": 65.36,
      "sgst": 65.36,
      "total_tax": 130.73,
      "final_amount": 857.03,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0935",
      "invoice_number": "UC610-2025-26-806",
      "invoice_date": "2026-02-09 12:19:17",
      "name": "Combos Saloon ",
      "customer_phone": "7836087861",
      "customer_gst": null,
      "address": "Vyapar Kendra Sushant lok 1  GF shop 206 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 382.2,
      "cgst": 34.4,
      "sgst": 34.4,
      "total_tax": 68.8,
      "final_amount": 451,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0936",
      "invoice_number": "UC610-2025-26-807",
      "invoice_date": "2026-02-09 12:27:19",
      "name": "KETAN ",
      "customer_phone": "8077192195",
      "customer_gst": null,
      "address": "TRADERS 728 SECTOR 43 , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 537.6,
      "cgst": 48.38,
      "sgst": 48.38,
      "total_tax": 96.77,
      "final_amount": 634.37,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0937",
      "invoice_number": "UC610-2025-26-808",
      "invoice_date": "2026-02-09 18:19:26",
      "name": "saloni",
      "customer_phone": "9140775773",
      "customer_gst": null,
      "address": "sector 43 susant lok , , vyapar kendra , vyapar kendra , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 119.2,
      "cgst": 10.73,
      "sgst": 10.73,
      "total_tax": 21.46,
      "final_amount": 140.66,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0939",
      "invoice_number": "UC610-2025-26-809",
      "invoice_date": "2026-02-09 19:07:47",
      "name": "Tarundeep Kaur",
      "customer_phone": "8696607881",
      "customer_gst": null,
      "address": "201, Rakshak apartment, The Retreat  South City 1, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 287.2,
      "cgst": 25.85,
      "sgst": 25.85,
      "total_tax": 51.7,
      "final_amount": 338.9,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0938",
      "invoice_number": "UC610-2025-26-810",
      "invoice_date": "2026-02-09 19:08:14",
      "name": "Barsha",
      "customer_phone": "9707996294",
      "customer_gst": null,
      "address": "House no 581 sector 42, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1098,
      "cgst": 98.82,
      "sgst": 98.82,
      "total_tax": 197.64,
      "final_amount": 1295.64,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0940",
      "invoice_number": "UC610-2025-26-811",
      "invoice_date": "2026-02-09 19:20:24",
      "name": "Mr. Shubrak",
      "customer_phone": "9315252436",
      "customer_gst": null,
      "address": "C2612 4th floor, Sushant Lok Block-C, Sector-43 Gurugram, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 852,
      "cgst": 76.68,
      "sgst": 76.68,
      "total_tax": 153.36,
      "final_amount": 1005.36,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0942",
      "invoice_number": "UC610-2025-26-812",
      "invoice_date": "2026-02-10 19:57:42",
      "name": "Ms. Shubangi",
      "customer_phone": "7073299185",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1463.38,
      "cgst": 131.71,
      "sgst": 131.71,
      "total_tax": 263.41,
      "final_amount": 1726.79,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0943",
      "invoice_number": "UC610-2025-26-813",
      "invoice_date": "2026-02-11 11:32:45",
      "name": "SUSHANT ",
      "customer_phone": "8851241641",
      "customer_gst": null,
      "address": "C 2386 BLOCK C, , VYAPAR KENDRA , VYAPAR KENDRA , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 628.2,
      "cgst": 56.54,
      "sgst": 56.54,
      "total_tax": 113.08,
      "final_amount": 741.28,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0945",
      "invoice_number": "UC610-2025-26-814",
      "invoice_date": "2026-02-11 16:51:14",
      "name": "nikhil ",
      "customer_phone": "9811843113",
      "customer_gst": null,
      "address": "sec 43, , house 851 3rd floor 301, house 851 3rd floor 301, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 356.8,
      "cgst": 32.11,
      "sgst": 32.11,
      "total_tax": 64.22,
      "final_amount": 421.02,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0946",
      "invoice_number": "UC610-2025-26-815",
      "invoice_date": "2026-02-11 16:53:59",
      "name": "ASHISH",
      "customer_phone": "8090897922",
      "customer_gst": null,
      "address": "SUSHANT LOK PHASE 1, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 83.9,
      "cgst": 7.55,
      "sgst": 7.55,
      "total_tax": 15.1,
      "final_amount": 99,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0944",
      "invoice_number": "UC610-2025-26-816",
      "invoice_date": "2026-02-11 18:26:06",
      "name": "Eshani",
      "customer_phone": "8130401440",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 718.2,
      "cgst": 64.64,
      "sgst": 64.64,
      "total_tax": 129.28,
      "final_amount": 847.48,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0947",
      "invoice_number": "UC610-2025-26-817",
      "invoice_date": "2026-02-11 18:36:47",
      "name": "ANKIT KUMAR",
      "customer_phone": "9318337348",
      "customer_gst": null,
      "address": "SECTOR 43, , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 446.48,
      "cgst": 40.19,
      "sgst": 40.19,
      "total_tax": 80.37,
      "final_amount": 526.85,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0950",
      "invoice_number": "UC610-2025-26-818",
      "invoice_date": "2026-02-12 11:06:32",
      "name": "Harshita",
      "customer_phone": "9521504626",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 698,
      "cgst": 62.82,
      "sgst": 62.82,
      "total_tax": 125.64,
      "final_amount": 823.64,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0951",
      "invoice_number": "UC610-2025-26-819",
      "invoice_date": "2026-02-12 11:07:24",
      "name": "Sameer Singal",
      "customer_phone": "9996712662",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 801.9,
      "cgst": 72.17,
      "sgst": 72.17,
      "total_tax": 144.34,
      "final_amount": 946.24,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0953",
      "invoice_number": "UC610-2025-26-820",
      "invoice_date": "2026-02-12 19:20:41",
      "name": "Utkarsh Mishra ",
      "customer_phone": "7007833075",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 405.32,
      "cgst": 36.48,
      "sgst": 36.48,
      "total_tax": 72.96,
      "final_amount": 478.28,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0954",
      "invoice_number": "UC610-2025-26-821",
      "invoice_date": "2026-02-12 19:23:42",
      "name": "Swayam Gautam",
      "customer_phone": "9824804333",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 852.07,
      "cgst": 76.68,
      "sgst": 76.68,
      "total_tax": 153.37,
      "final_amount": 1005.44,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0955",
      "invoice_number": "UC610-2025-26-822",
      "invoice_date": "2026-02-12 19:27:54",
      "name": "Ishan Sandhu ",
      "customer_phone": "9813941300",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1102.96,
      "cgst": 99.26,
      "sgst": 99.26,
      "total_tax": 198.53,
      "final_amount": 1301.49,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0961",
      "invoice_number": "UC610-2025-26-823",
      "invoice_date": "2026-02-14 18:06:56",
      "name": "Raghav Varma",
      "customer_phone": "8727909140",
      "customer_gst": null,
      "address": "chakarpur sector 28, , 264, 264, ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 301.29,
      "cgst": 27.11,
      "sgst": 27.11,
      "total_tax": 54.23,
      "final_amount": 355.52,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0962",
      "invoice_number": "UC610-2025-26-824",
      "invoice_date": "2026-02-14 18:46:22",
      "name": "surya",
      "customer_phone": "8448271143",
      "customer_gst": null,
      "address": "SECTOR 53 DLF GURGAON , , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 55,
      "cgst": 4.95,
      "sgst": 4.95,
      "total_tax": 9.9,
      "final_amount": 64.9,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0964",
      "invoice_number": "UC610-2025-26-825",
      "invoice_date": "2026-02-15 13:00:07",
      "name": "MR . RAJESH ",
      "customer_phone": "9810806793",
      "customer_gst": null,
      "address": "Orlov court4 / 801A Essel tower., , , , ",
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 2195.2,
      "cgst": 197.57,
      "sgst": 197.57,
      "total_tax": 395.14,
      "final_amount": 2590.34,
      "payment_status": "Pending"
    },
    {
      "order_number": "UC610-0965",
      "invoice_number": "UC610-2025-26-826",
      "invoice_date": "2026-02-15 17:52:23",
      "name": "Mayank",
      "customer_phone": "9878945424",
      "customer_gst": null,
      "address": null,
      "store_address": "Shop No. 176, Ground Floor, Vayapar Kendra Market, Sushant Lok-1, Gurugram, Haryana - 122002",
      "city_name": "Gurugram",
      "taxable_value": 1159.09,
      "cgst": 104.32,
      "sgst": 104.32,
      "total_tax": 208.64,
      "final_amount": 1367.73,
      "payment_status": "Pending"
    }
  ],
  "status": "success"
}

Then using order_number from above results, I call following:

[this is a developer tools console level command that worked for me]

(async () => {
  const url = "/api/v1/bookings/search?query=UC610-0035&sortQuery=&page=1&filterQuery=&type=";

  const res = await fetch(url, {
    method: "GET",
    headers: {
      "Accept": "application/json, text/plain, */*",
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NTQ2LCJpYXQiOjE3NzExNTgzMjUsImV4cCI6MTc3Mzc1MDMyNX0.-i1qfOc6pevoK6P1B2jTCJsoSN_Pd2i2Wg9SunNWc2o",
    },
    credentials: "include",
  });

  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }

  // IMPORTANT: return an object so DevTools shows it as the final value
  return { status: res.status, finalUrl: res.url, data };
})()

Then above command returns

[
    {
        "pickup_time": "",
        "suggestions": "",
        "created_at": "2025-05-31 15:39:05",
        "payment_status": 1,
        "booking_code": "UC610-0035",
        "invoice_id": "UC610-2025-26-23",
        "orderthrough_id": 1,
        "final_amount": 526,
        "drop_rider_assign_at": "2025-05-31 18:30:37",
        "pickup_date": "0000-00-00",
        "in_process_at": "2025-05-31 15:50:12",
        "delivery_date": "2025-05-31",
        "name": "SHIVAM ",
        "id": 746219,
        "mobile": "9857170900",
        "email": "",
        "status": 7,
        "address": " Second floor, H.No.-220, sec-27, Second floor, 220,  , , , , "
    }
]

And from this object we are interest in "id" that will be used to invoke generateInvoice



---





## Project / Area

* Repo:** **`simplify_downloader`
* Pipeline:** **`app.crm_downloader.uc_orders_sync`
* Goal discussed: introduce a** ****parallel experimental API-based UC flow** (without breaking existing production flow), compare old vs new outputs, then eventually retire old path after parity confidence.

---

## Business Problem You Identified

* Current archive API entrypoint (`getDeliveredOrders`) only surfaces orders after a later lifecycle stage (not ideal for early order enrichment).
* You need early availability of:
  * customer/order details
  * service details
* and later availability of:
  * payment details
* You demonstrated a viable API chain:
  1. `generateGST` gives early order rows
  2. `bookings/search?query=<order_number>`gives booking** **`id`
  3. `generateInvoice/{id}` gives full invoice HTML/details

---

## What Was Implemented (High-level)

### 1) Existing flow kept intact

No replacement/removal of legacy path:

* UI GST download
* archive API extraction
* existing excel/ingest/publish behavior

### 2) Experimental sub-path added (env-gated)

After Playwright login + dashboard readiness, experimental path can run if:

* `UC_GST_API_EXPERIMENT_ENABLED=true`

It generates separate “exp” artifacts.

### 3) Comparator module added

A dedicated compare step was added to evaluate old vs new results for migration safety.

---

## Modules / Files Added or Updated

### Added

* `app/crm_downloader/uc_orders_sync/gst_api_extract.py`
  * API extraction for GST-based experimental path
  * booking lookup + invoice parsing
  * experimental row models for gst/base/order/payment outputs
* `app/crm_downloader/uc_orders_sync/extract_comparator.py`
  * comparison summary + detailed mismatch coverage
* `tests/crm_downloader/test_uc_extract_comparator.py`
  * comparator behavior tests
* `tests/crm_downloader/test_no_merge_conflict_markers.py`
  * regression guard for unresolved merge markers in** **`uc_orders_sync`

### Updated

* `app/crm_downloader/uc_orders_sync/main.py`
  * wires experimental path into orchestration
  * writes experimental output files
  * API-only GST extraction (no compare artifact)

---

## Experimental Output Files (Current)

With** **`UC_GST_API_EXPERIMENT_ENABLED=true`, experimental outputs now include** ** **4 datasets** :

1. `*-exp_gst_api_gst_*.xlsx`
2. `*-exp_gst_api_base_order_info_*.xlsx`
3. `*-exp_gst_api_order_details_*.xlsx`
4. `*-exp_gst_api_payment_details_*.xlsx`

---

## How to Run (same script as before)

Use your normal script, just turn on experimental mode:

<pre class="overflow-visible! px-0!" data-start="2973" data-end="3103"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-[calc(var(--sticky-padding-top)+9*var(--spacing))]"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-bash"><span><span>UC_GST_API_EXPERIMENT_ENABLED=</span><span>true</span><span> \
./scripts/run_local_uc_orders_sync.sh --from-date 2026-02-01 --to-date 2026-02-15
</span></span></code></div></div></pre>

---

## Incident and Mitigation

### Issue hit

* You reported** **`IndentationError` due to unresolved merge conflict markers (`<<<<<<< ...`) in your local file after conflict-heavy PR integration.

### Mitigation done

* Added test:
  * `tests/crm_downloader/test_no_merge_conflict_markers.py`
* It fails if any conflict markers exist in:
  * `app/crm_downloader/uc_orders_sync/**/*.py`

This is a safety net for future merges/rebases.

---

## Important Caveat (for next session)

Experimental payment rows are generated from booking-search context/status heuristics and may not be equivalent to legacy archive-payment extraction in all cases.
So the payment comparison is end-to-end structurally, but you may still want to tighten semantic parity rules in the next iteration.

---

## Intent for Next Session

* Continue improving API-path parity and correctness using compare JSON output as acceptance signal.
* Once mismatch rates are acceptable, migrate from dual-run to API-primary and retire legacy pieces gradually.
