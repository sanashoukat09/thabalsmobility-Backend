import os
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from typing import Optional
import unicodedata
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins - beware security implications
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/")
def read_root():
    return {"message": "Backend is running."}

@app.post("/filter-driver")
async def filter_driver(
    file: UploadFile = File(...),
    driver_name: str = Form(...),
    add_break: bool = Form(False),
    give_off: bool = Form(False),
    break_start: Optional[str] = Form(None),
    break_end: Optional[str] = Form(None),
    off_date: Optional[str] = Form(None),
):
    try:
        # Validate file size
        if file.size > 10 * 1024 * 1024:  # 10 MB limit
            return JSONResponse(status_code=400, content={"error": "File size exceeds 10 MB limit."})

        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        # Store original column names
        original_columns = df.columns.tolist()
        normalized_columns = [col.strip().lower() for col in original_columns]
        df.columns = normalized_columns

        print("? Uploaded Columns:", df.columns.tolist())
        print(f"? Initial row count: {len(df)}")

        # Mapping German columns to internal names
        column_mapping = {
            "datum der fahrt": "date",
            "fahrername": "driver_name",
            "uhrzeit des fahrtbeginns": "ride_start",
            "uhrzeit des fahrtendes": "ride_end",
            "abholort": "pickup_location"  
        }

        for original, renamed in column_mapping.items():
            for col in df.columns:
                if col.strip().lower() == original.strip().lower():
                    df.rename(columns={col: renamed}, inplace=True)

        # Ensure required columns exist
        required_columns = {"driver_name", "ride_start", "ride_end", "date"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            return JSONResponse(
                status_code=400,
                content={"error": f"Excel file missing required columns: {', '.join(missing_columns)}"}
            )

        
        unique_drivers = df["driver_name"].astype(str).str.lower().str.strip().unique().tolist()
        print(f"? Unique Fahrername values: {unique_drivers}")

        # Parse datetime columns with extended format support
        datetime_formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%H:%M:%S.%f",
            "%H:%M:%S",
            "%d.%m.%Y %H:%M:%S.%f",
            "%d/%m/%Y %H:%M:%S.%f",
            "%d.%m.%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M",
            "%d/%m/%Y %H:%M",
            "%d.%m.%Y",
            "%d/%m/%Y"
        ]
        for col in ["ride_start", "ride_end"]:
            for fmt in datetime_formats:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
                if not df[col].isna().all():
                    break
            if df[col].isna().all():
                print(f"?? All {col} values failed to parse with formats: {datetime_formats}")
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce").dt.date
        if "datetime" in df.columns:
            for fmt in datetime_formats:
                df["datetime"] = pd.to_datetime(df["datetime"], format=fmt, errors="coerce")
                if not df["datetime"].isna().all():
                    break
            if df["datetime"].isna().all():
                print(f"?? All datetime (Datum/Uhrzeit) values failed to parse with formats: {datetime_formats}")

        # Log invalid datetime rows
        invalid_rows = df[["ride_start", "ride_end", "driver_name", "date"]].isna().any(axis=1)
        print(f"?? Rows with invalid datetimes: {invalid_rows.sum()}")
        if invalid_rows.any():
            print("?? Invalid rows sample:", df[invalid_rows][["driver_name", "ride_start", "ride_end", "date"]].head().to_dict())
        df.dropna(subset=["ride_start", "ride_end", "driver_name", "date"], inplace=True)
        print(f"? Rows after datetime parsing: {len(df)}")

        if df.empty:
            return JSONResponse(status_code=404, content={"error": "No valid data after parsing datetimes. Ensure Fahrername, Datum der Fahrt, ride_start, and ride_end have correct formats (e.g., YYYY-MM-DD for dates, HH:MM:SS.FFF for times)."})

        # Filter by driver name
        driver_name_clean = unicodedata.normalize("NFKD", driver_name.lower().strip())
        df["driver_name_normalized"] = df["driver_name"].astype(str).str.lower().str.strip().apply(lambda x: unicodedata.normalize("NFKD", x))
        df = df[df["driver_name_normalized"] == driver_name_clean]
        df.drop(columns=["driver_name_normalized"], inplace=True)
        print(f"? Rows after driver filter ('{driver_name_clean}'): {len(df)}")
        print(f"? Sample filtered rows: {df[['driver_name', 'date', 'ride_start', 'ride_end']].head().to_dict()}")
        if df.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data found for driver '{driver_name}'. Check spelling, case, whitespace, or special characters in Fahrername. Available drivers: {unique_drivers}."}
            )

        # Apply off day if specified
        if give_off and off_date:
            try:
                off_date = pd.to_datetime(off_date, format="%Y-%m-%d").date()
                pre_off_rows = len(df)
                df = df[df["date"] != off_date]
                print(f"? Removed {pre_off_rows - len(df)} rows for off day on {off_date}, remaining: {len(df)}")
            except ValueError as e:
                return JSONResponse(status_code=400, content={"error": f"Invalid off date format. Use YYYY-MM-DD. Error: {e}"})

        # Apply break time filtering if specified
        if add_break and break_start and break_end:
            try:
                break_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
                break_start_dt = None
                break_end_dt = None
                for fmt in break_formats:
                    try:
                        break_start_dt = pd.to_datetime(break_start, format=fmt, errors="raise")
                        break_end_dt = pd.to_datetime(break_end, format=fmt, errors="raise")
                        break
                    except ValueError:
                        continue
                if break_start_dt is None or break_end_dt is None:
                    raise ValueError("Invalid break time format. Use YYYY-MM-DD HH:MM:SS.FFF.")
                if (break_end_dt - break_start_dt).total_seconds() <= 0:
                    return JSONResponse(status_code=400, content={"error": "Break end time must be after start time."})
                pre_break_rows = len(df)
                df = df[
                    ~(
                        (df["date"] == break_start_dt.date()) &
                        (df["ride_start"] <= break_end_dt) &
                        (df["ride_end"] >= break_start_dt)
                    )
                ]
                print(f"? Removed {pre_break_rows - len(df)} rows for break on {break_start_dt.date()} from {break_start_dt.time()} to {break_end_dt.time()}, remaining: {len(df)}")
            except ValueError as e:
                return JSONResponse(status_code=400, content={"error": f"Invalid break time format. Use YYYY-MM-DD HH:MM:SS.FFF (24-hour). Error: {e}"})

        if df.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data remains for driver '{driver_name}' after filtering. Check off date or break times against Datum der Fahrt and ride times, or verify '{driver_name}' has valid data."}
            )

        # Calculate hours worked
        df["hours_worked"] = (df["ride_end"] - df["ride_start"]).dt.total_seconds() / 3600

        # Replace Abholort for the first ride of each day per driver, if Abholort exists
        if "pickup_location" in df.columns:
            for (driver, date), group in df.groupby(["driver_name", "date"]):
                if not group.empty:
                    earliest_ride_idx = group["ride_start"].idxmin()
                    df.loc[earliest_ride_idx, "pickup_location"] = "Gladbacher Strasse 189, 41747 Viersen, Germany"
            print(f"? Updated Abholort for first rides: {df[df['pickup_location'] == 'Gladbacher Strasse 189, 41747 Viersen, Germany'].shape[0]} rows updated")
        else:
            print("?? Abholort column not found in input Excel. Skipping pickup location update.")

        # Prepare filtered DataFrame, excluding 'datetime'
        filtered = df.copy()
        if "datetime" in filtered.columns:
            filtered = filtered.drop(columns=["datetime"])
        print(f"? Rows in filtered DataFrame: {len(filtered)}")
        print(f"? Output columns: {filtered.columns.tolist()}")

        # Format datetime columns to exactly .000 milliseconds
        for col in ["ride_start", "ride_end"]:
            filtered[col] = filtered[col].dt.strftime("%Y-%m-%d %H:%M:%S.000")

        # Format date column to ensure YYYY-MM-DD
        if "date" in filtered.columns:
            filtered["date"] = pd.to_datetime(filtered["date"]).dt.strftime("%Y-%m-%d")

        # Restore original column names, excluding 'datetime'
        original_columns_no_datetime = [col for col in original_columns if col.strip().lower() != "datum/uhrzeit"]
        final_columns = [
            original_columns_no_datetime[normalized_columns.index(col)] if col in normalized_columns and col != "datetime" else col
            for col in filtered.columns
        ]
        filtered.columns = final_columns

        # Export result with explicit column widths and formats
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            filtered.to_excel(writer, index=False, sheet_name="Sheet1")
            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]

            # Set column widths and formats
            for idx, col in enumerate(filtered.columns, 1):
                column_letter = get_column_letter(idx)
                if col.lower() == "datum der fahrt":
                    worksheet.column_dimensions[column_letter].width = 15  # Wide enough for YYYY-MM-DD
                    for cell in worksheet[column_letter][1:]:  # Skip header
                        cell.number_format = "YYYY-MM-DD"
                elif col.lower() in ["uhrzeit des fahrtbeginns", "uhrzeit des fahrtendes"]:
                    worksheet.column_dimensions[column_letter].width = 25  # Wide enough for YYYY-MM-DD HH:MM:SS.000
                    for cell in worksheet[column_letter][1:]:  # Skip header
                        cell.number_format = "YYYY-MM-DD HH:MM:SS.000"
                elif col.lower() == "hours_worked":
                    worksheet.column_dimensions[column_letter].width = 12
                    for cell in worksheet[column_letter][1:]:  # Skip header
                        cell.number_format = "0.00"
                elif col.lower() == "abholort":
                    worksheet.column_dimensions[column_letter].width = 40  # Wide enough for address
                    for cell in worksheet[column_letter][1:]:  # Skip header
                        cell.number_format = "@"  # Text format for address
                else:
                    worksheet.column_dimensions[column_letter].width = 20  # Default for other columns

        output.seek(0)

        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename=filtered_{driver_name_clean}.xlsx"}
        )

    except Exception as e:
        print(f"? Error: {e}")
        return JSONResponse(status_code=500, content={"error": f"Server error: {str(e)}"})


