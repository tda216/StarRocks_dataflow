CREATE TABLE IF NOT EXISTS hotel_booking.raw_hotel_bookings (
    hotel VARCHAR(64) NOT NULL,
    is_canceled VARCHAR(8) NULL,
    lead_time VARCHAR(32) NULL,
    arrival_date_year VARCHAR(16) NOT NULL,
    arrival_date_month VARCHAR(32) NULL,
    arrival_date_week_number VARCHAR(16) NULL,
    arrival_date_day_of_month VARCHAR(16) NULL,
    stays_in_weekend_nights VARCHAR(16) NULL,
    stays_in_week_nights VARCHAR(16) NULL,
    adults VARCHAR(16) NULL,
    children VARCHAR(16) NULL,
    babies VARCHAR(16) NULL,
    meal VARCHAR(32) NULL,
    country VARCHAR(32) NULL,
    market_segment VARCHAR(64) NULL,
    distribution_channel VARCHAR(64) NULL,
    is_repeated_guest VARCHAR(8) NULL,
    previous_cancellations VARCHAR(16) NULL,
    previous_bookings_not_canceled VARCHAR(16) NULL,
    reserved_room_type VARCHAR(16) NULL,
    assigned_room_type VARCHAR(16) NULL,
    booking_changes VARCHAR(16) NULL,
    deposit_type VARCHAR(32) NULL,
    agent VARCHAR(64) NULL,
    company VARCHAR(64) NULL,
    days_in_waiting_list VARCHAR(16) NULL,
    customer_type VARCHAR(64) NULL,
    adr VARCHAR(32) NULL,
    required_car_parking_spaces VARCHAR(16) NULL,
    total_of_special_requests VARCHAR(16) NULL,
    reservation_status VARCHAR(32) NULL,
    reservation_status_date VARCHAR(32) NULL,
    source_file VARCHAR(512) NULL,
    loaded_at DATETIME NULL
)
ENGINE=OLAP
DUPLICATE KEY(hotel)
DISTRIBUTED BY HASH(hotel) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);
