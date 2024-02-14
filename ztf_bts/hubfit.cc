#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <sstream>
#include <cmath>

#include <mpi.h>
#include <spdlog/spdlog.h>

#include "argparser.hh"
#include "stumbler.hh"


// Next 3 stolen from https://stackoverflow.com/questions/216823/how-to-trim-a-stdstring

static inline void ltrim( std::string &s ) {
  s.erase( s.begin(), std::find_if( s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); } ) );
}

static inline void rtrim( std::string &s ) {
  s.erase( std::find_if( s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); } ).base(), s.end() );
}

static inline void trim( std::string &s ) {
  rtrim(s);
  ltrim(s);
}

static std::vector<std::string> splitstring( const std::string& line ) {
  std::vector<std::string> tokens;
  int dex = 0;
  
  while ( dex < line.length() ) {
    std::string token("");
    size_t nextcomma = line.find( ",", dex );
    if ( nextcomma == std::string::npos ) {
      token = line.substr( dex, std::string::npos );
      dex = line.length();
    }
    else {
      token = line.substr( dex, nextcomma-dex );
      dex = nextcomma + 1;
    }
    trim( token );
    tokens.push_back( token );
  }

  return tokens;
}

// ======================================================================

class SNData {
  std::vector<double> z;
  std::vector<double> dz;
  std::vector<double> mbstar;
  std::vector<double> dmbstar;
  std::vector<double> x1;
  std::vector<double> dx1;
  std::vector<double> c;
  std::vector<double> dc;
  std::vector<double> chisq;
  std::vector<int> dof;

  double intrinsic_dm;
  
public:
  std::vector<std::string> sn;
  double max_log_likelihood;
  double min_chisq;
  
  SNData( double intrinsic_dm )
    : intrinsic_dm( intrinsic_dm )
  {
    max_log_likelihood = -1e30;
    min_chisq = 1e30;
  };

  // ----------------------------------------
  
  void read_datafile( const std::string& filename, bool reject ) {
    std::vector<std::string> kws{ "sn", "z", "dz", "mbstar", "dmbstar", "x1", "dx1", "c", "dc", "chisq", "dof" };
    std::map<std::string,int> dexen;
    for ( auto kw = kws.cbegin(); kw != kws.cend() ; ++kw ) dexen[*kw] = -1;
    
    const int bufsize = 1024;
    char buf[1024];
    
    std::ifstream ifp;
    ifp.open( filename );

    // Parse header
    ifp.getline( buf, bufsize );
    std::vector<std::string> hdr = splitstring( std::string( buf ) );
    for ( int i = 0 ; i < hdr.size() ; ++i ) {
      for ( auto kw = kws.cbegin() ; kw != kws.cend() ; ++kw ) {
        if ( *kw == hdr[i] ) {
          if ( dexen[*kw] >= 0 ) {
            // LOG ERROR
            throw std::runtime_error( "A keyword appears more than once." );
          }
          dexen[*kw] = i;
        }
      }
    }
    for ( auto kw = kws.cbegin() ; kw != kws.cend() ; ++kw ) {
      if ( dexen[*kw] < 0 ) {
        throw std::runtime_error( "Didn't find all keywords." );
      }
    }

    sn.clear();
    z.clear();
    dz.clear();
    mbstar.clear();
    dmbstar.clear();
    x1.clear();
    dx1.clear();
    c.clear();
    dc.clear();
    chisq.clear();
    dof.clear();
    
    std::stringstream str( "" );
    std::string sval;
    int ival;
    double fval;
    int ntot = 0;
    int nc = 0;
    int nx1 = 0;
    int ndx1 = 0;
    int ndc = 0;
    while ( !ifp.eof() ) {
      ifp.getline( buf, bufsize );
      auto line = std::string( buf );
      trim( line );
      if ( line.length() == 0 ) continue;
      if ( line[0] == '#' ) continue;

      ntot += 1;        
      std::vector<std::string> strfields = splitstring( line );
      std::vector<double> fields( strfields.size() );
      for ( int i = 0 ; i < strfields.size() ; ++i ) {
        if ( i != dexen["sn"] ) fields[i] = std::stod( strfields[i] );
      }

      // Cuts from Popovic et all, ApJ, 2021, 913, 49
      bool good = true;
      if ( reject ) {
        if ( fields[dexen["c"]] > 0.3 || fields[dexen["c"]] < -0.3 ) {
            nc += 1;
            good = false;
          }
        if ( fields[dexen["x1"]] > 3 || fields[dexen["x1"]] < -3 ) {
            nx1 += 1;
            good = false;
          }
        if ( fields[dexen["dc"]] > 0.2 ) {
          ndx1 += 1;
          good = false;
        }
        if ( fields[dexen["dx1"]] > 1 ) {
          ndc += 1;
          good = false;
        }
      }
      if ( ! good ) continue;

      sn.push_back( strfields[dexen["sn"]] );
      z.push_back( fields[dexen["z"]] );
      dz.push_back( fields[dexen["dz"]] );
      mbstar.push_back( fields[dexen["mbstar"]] );
      dmbstar.push_back( fields[dexen["dmbstar"]] );
      x1.push_back( fields[dexen["x1"]] );
      dx1.push_back( fields[dexen["dx1"]] );
      c.push_back( fields[dexen["c"]] );
      dc.push_back( fields[dexen["dc"]] );
      chisq.push_back( fields[dexen["chisq"]] );
      dof.push_back( (int)(fields[dexen["dof"]]) );
    }

    ifp.close();

    str.str("");
    str << "Kept " << sn.size() << " out of " << ntot << " ; "
        << nc << " bad c, " << nx1 << " bad x1, " << ndc << " bad dc, " << ndx1 << " bad dx1.";
    spdlog::info( str.str() );
    
  }

  // ----------------------------------------
  // param are:
  //   0 : α
  //   1 : β
  //   2 : script-M
  
  double lnL( const std::vector<double>& param ) {
    // likelihood is
    //   (2π)^(-n/2) * det(C)^(-1/2) * exp( -1/2 * dT · C¯¹· d )
    // where n is the number of data points, C is the covariance matrix,
    // d is data-model.
    //
    // We're ignoring covariances, so C is diagonal, meaning det(C)
    // is just the product of the variances, and dT · C⁻¹ · d is just
    // sum( d² / σ² ).
    //
    // so lnL is (-n/2)*ln(2π) - 1/2 * sum( ln(σ²) ) - 1/2 * sum( d² / σ² )
    
    double loglikelihood = -(sn.size() / 2.) * log( 2*M_PI );
    double chisq = 0;
    double val;

    for ( auto i = 0 ; i < sn.size() ; ++i ) {
      double mbfit = param[2] + 5 * log10( z[i] ) - param[0] * x1[i] + param[1] * c[i];
      double diff = mbstar[i] - mbfit;
      // Assume neglible error in z
      double val0 = dmbstar[i];
      double val1 = param[0] * dx1[i];
      double val2 = param[1] * dc[i];
      double sigma2 = val0*val0 + val1*val1 + val2*val2 + intrinsic_dm*intrinsic_dm;

      chisq += ( diff*diff ) / sigma2;
      loglikelihood -= 1./2. * ( log(sigma2) + ( diff*diff / sigma2 ) );
    }

    if ( chisq < min_chisq )
      min_chisq = chisq;
    
    if ( loglikelihood > max_log_likelihood )
      max_log_likelihood = loglikelihood;
    
    return loglikelihood;
  }

};

// ======================================================================

int main( int argc, char* argv[] ) {
  int provided = 0;
  int err = MPI_Init_thread( &argc, &argv, MPI_THREAD_MULTIPLE, &provided );
  assert( err == 0 );
  assert( provided == MPI_THREAD_MULTIPLE );

  int mpirank, mpisize;

  MPI_Comm_rank( MPI_COMM_WORLD, &mpirank );
  MPI_Comm_size( MPI_COMM_WORLD, &mpisize );

  if ( mpisize != 1 ) {
    spdlog::error( "Error, assuming mpisize 1 in places." );
    exit( 20 );
  }
  
  auto argparser = ArgParser();
  Argument<std::string> arg_filename( "", "", "filename", "data.csv", "Data file" );
  argparser.add_arg( arg_filename );
  Argument<bool> arg_help( "-h", "--help", "help", false );
  argparser.add_arg( arg_help );
  Argument<int> arg_seed( "", "--seed", "seed", 0, "Random seed for Stumbler; 0=use system entropy" );
  argparser.add_arg( arg_seed );
  Argument<int> arg_nwalkers( "-n", "--nwalkers", "nwalkers", 100, "Number of walkiers" );
  argparser.add_arg( arg_nwalkers );
  Argument<int> arg_nsteps( "-s", "--steps", "nsteps", 200, "Number of steps after burn-in" );
  argparser.add_arg( arg_nsteps );
  Argument<int> arg_burnin( "-b", "--burnin", "burnin", 100, "Number of burn-in steps" );
  argparser.add_arg( arg_burnin );
  Argument<double> arg_intdm( "-i", "--intrinsic-dm", "intrinsic_dm", 0.1, "Intrinsic magnitude scatter" );
  argparser.add_arg( arg_intdm );
  Argument<double> arg_stretchparam( "-z", "--stretchparam", "stretchparam", 2.0,
                                     "Goodman & Weare stretch parameter" );
  argparser.add_arg( arg_stretchparam );
  Argument<bool> arg_verbose( "-v", "--verbose", "verbose", false, "Show debug log info" );
  argparser.add_arg( arg_verbose );
  Argument<bool> arg_reject( "-r", "--reject", "reject", false, "Reject |c|>0.3, |x1|>3, dc>0.2, dx1>1" );
  argparser.add_arg( arg_reject );
  
  argparser.parse( argc, argv );

  if ( arg_help.given ) {
    if ( mpirank == 0 ) {
      std::cout << argparser.help() << std::endl;
    }
    MPI_Barrier( MPI_COMM_WORLD );
    MPI_Finalize();
    exit( 0 );
  }

  SNData sndata( arg_intdm.get_val() );
  sndata.read_datafile( arg_filename.get_val(), arg_reject.given );

  std::vector<double> initparam{ 0.14, 3.2, 24 };
  std::vector<double> initsigma{ 0.01, 0.1, 0.1 };
  unsigned long int seed = arg_seed.get_val();
  
  std::function<double(const std::vector<double>&)> lnLfunc = std::bind( &SNData::lnL, &sndata,
                                                                         std::placeholders::_1 );
  Stumbler stumbler( arg_nwalkers.get_val(), arg_burnin.get_val(), arg_nsteps.get_val(), arg_stretchparam.get_val(),
                     initparam, initsigma, lnLfunc, MPI_COMM_WORLD, seed );
  stumbler.go( 100 );

  std::ofstream ofp;
  ofp.open( "chain.dat", std::ios_base::out );
  ofp << "alpha beta scriptm" << std::endl;
  for ( auto link = stumbler.chain.begin() ; link != stumbler.chain.end() ; ++link ) {
    ofp << (*link)[0] << " " << (*link)[1] << " " << (*link)[2] << std::endl;
  }
  ofp.close();

  std::cout << "There are " << sndata.sn.size() << " data points.\n";
  std::cout << "Max log likelihood: " << sndata.max_log_likelihood << std::endl;
  std::cout << "Min χ²: " << sndata.min_chisq << std::endl;
  
  MPI_Finalize();
  exit( 0 );
}
