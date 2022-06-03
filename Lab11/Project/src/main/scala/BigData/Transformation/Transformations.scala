package BigData.Transformation

import BigData.Case.{Actors, Rentals}

object Transformations {
  def isActorWithCharacters(actors_row: Actors): Boolean ={
    return actors_row.category=="actor" && actors_row.characters!=null;
  }

  def isNotWorkingday(rentals_row: Rentals) : Boolean ={
    return rentals_row.workingday=="0";
  }

}
