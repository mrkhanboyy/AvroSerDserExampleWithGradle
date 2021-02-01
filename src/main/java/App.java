import com.avro.model.Car;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

public class App {

    public static void main(String[] args) throws IOException {
//        Car c = Car.newBuilder().setBrandName("BMW")
//                .setRegNo("RN_1212")
//                .setModelNo("MN-2019")
//                .setChasisNo("CN-1900")
//                .setNumberOfWheels(4)
//                .setCurrentSpeed(60.20)
//                .setPrice(5000000L)
//                .setIsLatestModel(true)
//                .build();
//
//        System.out.println(c);

        File f = new File("/home/rizwan/Desktop/avro/serializedObject.avro");
//        DatumWriter<Car> userDatumWriter = new SpecificDatumWriter<Car>(Car.class);
//        try (DataFileWriter<Car> dataFileWriter = new DataFileWriter<Car>(userDatumWriter)) {
//            dataFileWriter.create(Car.getClassSchema(), f);
//            dataFileWriter.append(c);
//            dataFileWriter.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println("object serialized successfully");

//        // Deserialize Users from disk
        DatumReader<Car> userDatumReader = new SpecificDatumReader<Car>(Car.class);
        DataFileReader<Car> dataFileReader = new DataFileReader<Car>(f, userDatumReader);
        Car car = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            car = dataFileReader.next(car);
            System.out.println(car);
        }

    }
}