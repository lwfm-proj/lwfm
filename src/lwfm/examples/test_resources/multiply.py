import sys

def multiply_file_numbers(input_file, multiplier, output_file):
    # Open the input file for reading and create the output file
    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        # Read each line from the input file
        for line in input_file:
            # Remove any leading or trailing whitespace
            line = line.strip()
            
            # Convert the line to a number and multiply by the multiplier
            number = float(line)  # Use int() instead of float() if dealing with integers
            multiplied_number = number * multiplier
            
            # Write the multiplied number to the output file
            output_file.write(str(multiplied_number) + '\n')

    print("Multiplication completed successfully. Output file:", output_file)


if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 4:
        print("Usage: python multiply_file.py <input_file> <multiplier> <output_file>")
        sys.exit(1)

    # Get the input file, multiplier, and output file from command-line arguments
    input_file = sys.argv[1]
    multiplier = float(sys.argv[2])  # Use int() instead of float() if dealing with integers
    output_file = sys.argv[3]

    # Call the function to multiply the file numbers
    multiply_file_numbers(input_file, multiplier, output_file)
