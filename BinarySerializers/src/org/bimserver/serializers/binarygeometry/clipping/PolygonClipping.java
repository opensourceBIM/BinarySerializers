package org.bimserver.serializers.binarygeometry.clipping;

/******************************************************************************
 * Copyright (C) 2009-2019  BIMserver.org
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see {@literal<http://www.gnu.org/licenses/>}.
 *****************************************************************************/

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Color;
import java.awt.Polygon;
import javax.swing.JPanel;
import java.util.ArrayList;
import java.util.Random;

public class PolygonClipping extends JPanel{

//main class, does the 'work'
public PolygonClipping(){
 //create input and window polygons and fill array lists
// input = generateInput();
// window = generateWindow();
//
// //generate output from S.H.-clipping algorithm
// output = clippingAlg(input,window);
// System.out.print("Clipped polygon from "+input.size());
// System.out.println(" to "+output.size()+" points.");
}


//creates the input polygon, random 3-300 vertices

//creates the window to clip the input polygon, random quadrilateral

//conducts the Sutherland-Hodgman clipping algorithm
//returns array list of points, points of the output polygon
public ArrayList<Point> clippingAlg(ArrayList subjectP,ArrayList clippingP){
 ArrayList<Edge> edges = clippingP;
 ArrayList<Point> in = subjectP;
 ArrayList<Point> out = new ArrayList<Point>();

 //begin looping through edges and points
 int casenum = 0;
 for(int i=0;i<edges.size();i++){
   Edge e = edges.get(i);
   Point r = edges.get((i+2)%edges.size()).getP1();
   Point s = in.get(in.size()-1);
   for(int j=0;j<in.size();j++){
     Point p = in.get(j);

     //first see if the point is inside the edge
     if(Edge.isPointInsideEdge(e,r,p)){
       //then if the specific pair of points is inside 
       if(Edge.isPointInsideEdge(e,r,s)){
         casenum = 1;
       //pair goes outside, so one point still inside
       }else{
         casenum = 4;
       }

     //no point inside
     }else{
       //does the specific pair go inside
       if(Edge.isPointInsideEdge(e,r,s)){
         casenum = 2;
       //no points in pair are inside
       }else{
         casenum = 3;
       }
     }

     switch(casenum){

       //pair is inside, add point
       case 1:
         out.add(p);
         break;

       //pair goes inside, add intersection only
       case 2:
         Point inter0 = e.computeIntersection(s,p);
         out.add(inter0);
         break;

       //pair outside, add nothing
       case 3:
         break;

       //pair goes outside, add point and intersection
       case 4:
         Point inter1 = e.computeIntersection(s,p);
         out.add(inter1);
         out.add(p);
         break;
     }
     s = p;
   }
   in = (ArrayList<Point>)out.clone(); 
   if (in.size() == 0) {
	   return in;
   }
   out.clear();
 }
// for(Point pt:in){
//   q.addPoint((int)pt.getX(),(int)pt.getY());
// }  
 return in;
}

}