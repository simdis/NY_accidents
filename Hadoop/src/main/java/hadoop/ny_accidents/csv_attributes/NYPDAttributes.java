package hadoop.ny_accidents.csv_attributes;

/**
 * Enumeration containing all the attributes of the NYPD dataset.
 * @author Simone Disabato
 * @author fusiled
 */
public enum NYPDAttributes {
    DATE(0),
    TIME(1),
    BOROUGH(2),
    ZIP_CODE(3),
    LATITUDE(4),
    LONGITUDE(5),
    LOCATION(6),
    ON_STREET_NAME(7),
    CROSS_STREET_NAME(8),
    OFF_STREET_NAME(9),
    NUMBER_OF_PERSONS_INJURED(10),
    NUMBER_OF_PERSONS_KILLED(11),
    NUMBER_OF_PEDESTRIANS_INJURED(12),
    NUMBER_OF_PEDESTRIANS_KILLED(13),
    NUMBER_OF_CYCLIST_INJURED(14),
    NUMBER_OF_CYCLIST_KILLED(15),
    NUMBER_OF_MOTORIST_INJURED(16),
    NUMBER_OF_MOTORIST_KILLED(17),
    CONTRIBUTING_FACTOR_VEHICLE_1(18),
    CONTRIBUTING_FACTOR_VEHICLE_2(19),
    CONTRIBUTING_FACTOR_VEHICLE_3(20),
    CONTRIBUTING_FACTOR_VEHICLE_4(21),
    CONTRIBUTING_FACTOR_VEHICLE_5(22),
    UNIQUE_KEY(23),
    VEHICLE_TYPE_CODE_1(24),
    VEHICLE_TYPE_CODE_2(25),
    VEHICLE_TYPE_CODE_3(26),
    VEHICLE_TYPE_CODE_4(27),
    VEHICLE_TYPE_CODE_5(28);

    private int id;

    private NYPDAttributes(int id)
    {
        this.id=id;
    }

    public int get()
    {
        return this.id;
    }
}
