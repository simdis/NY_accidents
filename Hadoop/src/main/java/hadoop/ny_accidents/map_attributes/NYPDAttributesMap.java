package hadoop.ny_accidents.map_attributes;

import java.util.EnumMap;

/**
 * A map between the NYPD dataset's attributes and the corresponding integer index.
 * @author Simone Disabato
 *
 */
public class NYPDAttributesMap {
    private static EnumMap<NYPDAttributes,Integer> attributes=null;
    
    public NYPDAttributesMap() {
        if (attributes!=null)
        {
            return;
        }
        attributes = new EnumMap<NYPDAttributes,Integer>(NYPDAttributes.class);
        
        attributes.put(NYPDAttributes.DATE, 0);
        attributes.put(NYPDAttributes.TIME,1);
        attributes.put(NYPDAttributes.BOROUGH,2);
        attributes.put(NYPDAttributes.ZIP_CODE,3);
        attributes.put(NYPDAttributes.LATITUDE,4);
        attributes.put(NYPDAttributes.LONGITUDE,5);
        attributes.put(NYPDAttributes.LOCATION,6);
        attributes.put(NYPDAttributes.ON_STREET_NAME,7);
        attributes.put(NYPDAttributes.CROSS_STREET_NAME,8);
        attributes.put(NYPDAttributes.OFF_STREET_NAME,9);
        attributes.put(NYPDAttributes.NUMBER_OF_PERSONS_INJURED,10);
        attributes.put(NYPDAttributes.NUMBER_OF_PERSONS_KILLED,11);
        attributes.put(NYPDAttributes.NUMBER_OF_PEDESTRIANS_INJURED,12);
        attributes.put(NYPDAttributes.NUMBER_OF_PEDESTRIANS_KILLED,13);
        attributes.put(NYPDAttributes.NUMBER_OF_CYCLIST_INJURED,14);
        attributes.put(NYPDAttributes.NUMBER_OF_CYCLIST_KILLED,15);
        attributes.put(NYPDAttributes.NUMBER_OF_MOTORIST_INJURED,16);
        attributes.put(NYPDAttributes.NUMBER_OF_MOTORIST_KILLED,17);
        attributes.put(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_1,18);
        attributes.put(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_2,19);
        attributes.put(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_3,20);
        attributes.put(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_4,21);
        attributes.put(NYPDAttributes.CONTRIBUTING_FACTOR_VEHICLE_5,22);
        attributes.put(NYPDAttributes.UNIQUE_KEY,23);
        attributes.put(NYPDAttributes.VEHICLE_TYPE_CODE_1,24);
        attributes.put(NYPDAttributes.VEHICLE_TYPE_CODE_2,25);
        attributes.put(NYPDAttributes.VEHICLE_TYPE_CODE_3,26);
        attributes.put(NYPDAttributes.VEHICLE_TYPE_CODE_4,27);
        attributes.put(NYPDAttributes.VEHICLE_TYPE_CODE_5,28);
    }
    
    public int getKey(NYPDAttributes a) {
        return attributes.get(a);
    }
    
}
