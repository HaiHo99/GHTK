package Week4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Join {
    public static class ProfessionKey implements WritableComparable<ProfessionKey>{
        public Text profession = new Text();
        public IntWritable recordType = new IntWritable();
        public static final IntWritable PEOPLE_RECORD = new IntWritable(1);
        public static final IntWritable SALARY_RECORD = new IntWritable(0);

        public ProfessionKey() {}
        public ProfessionKey(String pro, int type) {
            this.profession.set(pro);
            this.recordType.set(type);
        }
        public boolean equals (ProfessionKey other) {
            return this.profession.equals(other.profession) && this.recordType.equals(other.recordType);

        }
        public int hashCode() {
            return this.profession.hashCode();
        }
        public void write(DataOutput dataOutput) throws IOException {
            this.profession.write(dataOutput);
            this.recordType.write(dataOutput);
        }
        public void readFields(DataInput dataInput) throws IOException {
            this.profession.readFields(dataInput);
            this.recordType.readFields(dataInput);
        }
        public int compareTo (ProfessionKey professionKey) {
            if (this.profession.equals(professionKey.profession)) {
                return this.recordType.compareTo(professionKey.recordType);
            } else {
                return this.profession.compareTo(professionKey.profession);
            }
        }
    }
    public static class PeopleRecord implements Writable {
        public IntWritable id = new IntWritable();
        public Text firstName = new Text();
        public Text lastName = new Text();
        public Text email = new Text();
        public Text city = new Text();
        public IntWritable fieldName = new IntWritable();

        public PeopleRecord() {}
        public PeopleRecord (int id, String firstName, String lastName, String email, String city, int fieldName) {
            this.id.set(id);
            this.firstName.set(firstName);
            this.lastName.set(lastName);
            this.email.set(email);
            this.city.set(city);
            this.fieldName.set(fieldName);
        }
        public void write(DataOutput dataOutput) throws IOException {
            this.id.write(dataOutput);
            this.firstName.write(dataOutput);
            this.lastName.write(dataOutput);
            this.email.write(dataOutput);
            this.city.write(dataOutput);
            this.fieldName.write(dataOutput);
        }
        public void readFields(DataInput dataInput) throws IOException {
            this.id.readFields(dataInput);
            this.firstName.readFields(dataInput);
            this.lastName.readFields(dataInput);
            this.email.readFields(dataInput);
            this.city.readFields(dataInput);
            this.fieldName.readFields(dataInput);
        }
    }
    public static class SalaryRecord implements Writable {
        LongWritable salary = new LongWritable();

        public SalaryRecord() {}
        public SalaryRecord(long salary) {
            this.salary.set(salary);
        }
        public void write(DataOutput dataOutput) throws IOException {
            this.salary.write(dataOutput);
        }
        public void readFields(DataInput dataInput) throws IOException {
            this.salary.readFields(dataInput);
        }
    }
    public static  class JoinGenericWritable extends GenericWritable {
        private static  Class<? extends Writable>[] CLASSES = null;

        static {
            CLASSES = (Class<?extends Writable>[]) new Class[] {
                    SalaryRecord.class,
                    PeopleRecord.class
            };
        }
        public JoinGenericWritable() {}
        public JoinGenericWritable(Writable instance) {
            set(instance);
        }
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }
    }
    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super(ProfessionKey.class,true);
        }
        public int compare(WritableComparable a, WritableComparable b) {
            ProfessionKey first = (ProfessionKey) a;
            ProfessionKey second = (ProfessionKey) b;
            return first.profession.compareTo(second.profession);
        }
    }
    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator() {
            super(ProfessionKey.class, true);
        }
        public int compare(WritableComparable a, WritableComparable b) {
            ProfessionKey first = (ProfessionKey) a;
            ProfessionKey second = (ProfessionKey) b;

            return first.compareTo(second);
        }
    }
    public static class PeopleMapper extends Mapper<LongWritable, Text, ProfessionKey, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws  IOException, InterruptedException {
            String[] recordFields =value.toString().split(",");

            int id = Integer.parseInt(recordFields[0]);
            String firstName = recordFields[1];
            String lastName = recordFields[2];
            String email = recordFields[3];
            String city = recordFields[4];
            String profession = recordFields[5];
            int fieldName = Integer.parseInt(recordFields[6]);

            ProfessionKey recordKey = new ProfessionKey(profession, ProfessionKey.PEOPLE_RECORD.get());
            PeopleRecord peopleRecord = new PeopleRecord(id,firstName,lastName,email,city,fieldName);
            JoinGenericWritable genericRecord = new JoinGenericWritable(peopleRecord);

            context.write(recordKey, genericRecord);
        }
    }
    public static class SalaryMapper extends Mapper<LongWritable, Text, ProfessionKey, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            String profession = recordFields[0];
            long salary = Integer.parseInt(recordFields[1]);
            SalaryRecord salaryRecord = new SalaryRecord(salary);
            ProfessionKey professionKey = new ProfessionKey(profession,ProfessionKey.SALARY_RECORD.get());
            JoinGenericWritable joinGenericWritable = new JoinGenericWritable(salaryRecord);
            context.write(professionKey,joinGenericWritable);
        }
    }
    public static class JoinReducer extends Reducer<ProfessionKey,JoinGenericWritable,NullWritable,Text> {
        private String header = "id,firstName,lastName,email,city,profession,fieldName,Salary";
        private Boolean is_header = true;
        public void reduce(ProfessionKey professionKey, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder output = new StringBuilder();
            String salary = "";
            if(is_header){
                context.write(NullWritable.get(),new Text(header));
                is_header = false;
            }


            for (JoinGenericWritable v : values){
                Writable record = v.get();
                if(professionKey.recordType.equals(ProfessionKey.SALARY_RECORD)){
                    SalaryRecord record1 = (SalaryRecord) record;
                    salary = record1.salary.toString();
                }else

                {
                    PeopleRecord record1 = (PeopleRecord)record;
                    output.append(record1.id).append(",");
                    output.append(record1.firstName).append(",");
                    output.append(record1.lastName).append(",");
                    output.append(record1.email).append(",");
                    output.append(record1.city).append(",");
                    output.append(professionKey.profession).append(",");
                    output.append(record1.fieldName).append(",");
                    output.append(salary);
                    context.write(NullWritable.get(),new Text(output.toString()));
                    output.delete(0,output.length());
                }
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "join");
        job.setJarByClass(Join.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(ProfessionKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PeopleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SalaryMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
